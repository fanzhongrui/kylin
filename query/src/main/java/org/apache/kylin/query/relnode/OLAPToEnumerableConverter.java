/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.kylin.query.relnode;

import java.util.List;

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.query.routing.NoRealizationFoundException;
import org.apache.kylin.query.routing.QueryRouter;
import org.apache.kylin.query.schema.OLAPTable;

/**
 * 当在Calcite中执行一个SQL时，Calcite会解析得到 AST 树，然后再对逻辑执行计划进行优化，
 *      Calcite的优化规则是基于规则的，在 Calcite 中注册了一些新的 Rule ，
 *      在优化的过程中会根据这些规则将算子转换为对应的物理执行算子，接下来 Calcite 从上到下一次执行这些算子。
 *      这些算子都实现了 EnumerableRel 接口，在执行的时候调用 implement 函数
 */

/**
 * 在所有Kylin优化之后的查询树中，根节点都是OLAPToEnumerableConverter
 */
public class OLAPToEnumerableConverter extends ConverterImpl implements EnumerableRel {

    public OLAPToEnumerableConverter(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
        super(cluster, ConventionTraitDef.INSTANCE, traits, input);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new OLAPToEnumerableConverter(getCluster(), traitSet, sole(inputs));
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // huge cost to ensure OLAPToEnumerableConverter only appears once in rel tree
        return planner.getCostFactory().makeCost(1E100, 0, 0);
    }

    /**
     * implement函数中首先根据每一个算子中保持的信息构造本次查询的上下文OLAPContext，
     *      例如根据OLAPAggregateRel 算子获取groupByColumns，
     *      根据OLAPFilterRel 算子将每次查询的过滤条件转换成TupleFilter
     *
     *  然后根据本次查询中使用的维度列（出现在groupByColumns和filterColumns中）、度量信息（aggregations）
     *      是否有满足本次查询的Cube，如果有则将其保存在 OLAPContext 的 realization 中， 获取数据时需要依赖于它。
     *
     *  然后在 rewrite 回调函数中递归调用每一个算子的 implementRewrite函数重新构造每一个算子的参数，
     *
     *  最后再调用每一个算子的 implementEnumerable 函数将其转换成EnumerableRef对象，
     *  这一步相当于将前面生成的物理执行计划再次转换成一个新的物理执行计划。
     * @param enumImplementor 算子
     * @param pref
     * @return
     */
    @Override
    public Result implement(EnumerableRelImplementor enumImplementor, Prefer pref) {
        // post-order travel children
        // 后序遍历子树
        OLAPRel.OLAPImplementor olapImplementor = new OLAPRel.OLAPImplementor();    //olap算子
        // 构造上下文 olapContext
        olapImplementor.visitChild(getInput(), this);

        // find cube from olap context
        // 从所有OLAP上下文 olapContext 中查找 cube ，
        // 选择可用IRealization（对于每一个context， 使用 QueryRouter 的 selectRealization函数选择）
        try {
            for (OLAPContext context : OLAPContext.getThreadLocalContexts()) {
                // Context has no table scan is created by OLAPJoinRel which looks like
                //     (sub-query) as A join (sub-query) as B
                // No realization needed for such context.

                if (context.firstTableScan == null) {
                    continue;
                }
                IRealization realization = QueryRouter.selectRealization(context);
                context.realization = realization;
            }
        } catch (NoRealizationFoundException e) {
            OLAPContext ctx0 = (OLAPContext) OLAPContext.getThreadLocalContexts().toArray()[0];
            if (ctx0 != null && ctx0.olapSchema.hasStarSchemaUrl()) {
                // generate hive result
                return buildHiveResult(enumImplementor, pref, ctx0);
            } else {
                throw e;
            }
        }

        // rewrite query if necessary
        // 构建RewriteImplementor算子
        OLAPRel.RewriteImplementor rewriteImplementor = new OLAPRel.RewriteImplementor();
        rewriteImplementor.visitChild(this, getInput());

        // implement as EnumerableRel
        OLAPRel.JavaImplementor impl = new OLAPRel.JavaImplementor(enumImplementor);
        EnumerableRel inputAsEnum = impl.createEnumerable((OLAPRel) getInput());
        this.replaceInput(0, inputAsEnum);

        if (System.getProperty("calcite.debug") != null) {
            String dumpPlan = RelOptUtil.dumpPlan("", this, false, SqlExplainLevel.DIGEST_ATTRIBUTES);
            System.out.println("EXECUTION PLAN AFTER REWRITE");
            System.out.println(dumpPlan);
        }

        //构建下一个物理执行计划
        return impl.visitChild(this, 0, inputAsEnum, pref);
    }

    private Result buildHiveResult(EnumerableRelImplementor enumImplementor, Prefer pref, OLAPContext context) {
        RelDataType hiveRowType = getRowType();

        context.setReturnTupleInfo(hiveRowType, null);
        PhysType physType = PhysTypeImpl.of(enumImplementor.getTypeFactory(), hiveRowType, pref.preferArray());

        RelOptTable factTable = context.firstTableScan.getTable();
        Result result = enumImplementor.result(physType, Blocks.toBlock(Expressions.call(factTable.getExpression(OLAPTable.class), "executeHiveQuery", enumImplementor.getRootExpression())));
        return result;
    }

}
