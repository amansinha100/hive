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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;

import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.plan.impala.HdfsScanNode;
import org.apache.hadoop.hive.ql.plan.impala.IdGenType;
import org.apache.hadoop.hive.ql.plan.impala.IdGenerator;
import org.apache.hadoop.hive.ql.plan.impala.PlanId;
import org.apache.hadoop.hive.ql.plan.impala.ScanNode;
import org.apache.hadoop.hive.ql.plan.impala.TupleDescriptor;
import org.apache.hadoop.hive.ql.plan.impala.TupleId;


import java.util.Map;
import java.util.List;
import com.google.common.collect.Lists;

/**
 * Impala specific transformation rules.
 */
public class HiveImpalaRules {

  /**
   * Rule to transform a Project-Filter-Scan logical plan into Impala's
   * HdfsScanNode that can process filters and projects directly within the scan
   */
  public static class ImpalaProjectFilterScanRule extends RelOptRule {
    private final Map<IdGenType, IdGenerator<?>> idGenerators_;

    public ImpalaProjectFilterScanRule(RelBuilderFactory relBuilderFactory,
                                       Map<IdGenType, IdGenerator<?>> idGenerators) {
      super(operand(HiveProject.class, operand(HiveFilter.class, operand(HiveTableScan.class, none()))),
              relBuilderFactory, null);
      idGenerators_ = idGenerators;

    }
    @Override public void onMatch(RelOptRuleCall call) {
      final HiveProject project = call.rel(0);
      final HiveFilter filter = call.rel(1);
      final HiveTableScan scan = call.rel(2);

      IdGenerator<TupleId> tupleIdGen = (IdGenerator<TupleId>) idGenerators_.get(IdGenType.TUPLE);
      IdGenerator<PlanId> planIdGen = (IdGenerator<PlanId>) idGenerators_.get(IdGenType.PLAN);

      // create an Impala TupleDescriptor
      TupleDescriptor tupleDesc = new TupleDescriptor(scan,
              getAllInputRefs(getRexNodes(project, filter)), tupleIdGen.getNextId(), idGenerators_);

      // Only support HDFS for now.
      // TODO: generalize to other scan types
      // NOTE: currently, the ScanNode is not a derived class of Calcite RelNode, so this will work
      // only after we create that hierarchy since the output of a 'Transform' rule should
      // be a RelNode type
      ScanNode newScan = new HdfsScanNode(scan.getCluster(), scan.getTraitSet(), tupleDesc,
              filter, planIdGen.getNextId());
      call.transformTo(newScan);
      return;

    }
  }
  /**
   * Rule to transform a Project-Scan logical plan into Impala's
   * HdfsScanNode that can process projects directly within the scan
   */
  public static class ImpalaProjectScanRule extends RelOptRule {

    private final Map<IdGenType, IdGenerator<?>> idGenerators_;

    public ImpalaProjectScanRule(RelBuilderFactory relBuilderFactory,
                                       Map<IdGenType, IdGenerator<?>> idGenerators) {
      super(operand(HiveProject.class, operand(HiveTableScan.class, none())),
              relBuilderFactory, null);
      idGenerators_ = idGenerators;
    }
    @Override public void onMatch(RelOptRuleCall call) {
      final HiveProject project = call.rel(0);
      final HiveTableScan scan = call.rel(1);

      IdGenerator<TupleId> tupleIdGen = (IdGenerator<TupleId>) idGenerators_.get(IdGenType.TUPLE);
      IdGenerator<PlanId> planIdGen = (IdGenerator<PlanId>) idGenerators_.get(IdGenType.PLAN);

      // create an Impala TupleDescriptor
      TupleDescriptor tupleDesc = new TupleDescriptor(scan,
              getAllInputRefs(getRexNodes(project, null /* no filter */)),
              tupleIdGen.getNextId(), idGenerators_);

      ScanNode newScan = new HdfsScanNode(scan.getCluster(), scan.getTraitSet(), tupleDesc,
              null /* no filter */, planIdGen.getNextId());
      call.transformTo(newScan);
      return;

    }
  }
    
  private static List<RexNode> getRexNodes(HiveProject project, HiveFilter filter) {
    List<RexNode> nodes = Lists.newArrayList();
    assert project != null;
    nodes.addAll(project.getProjects());
    if (filter != null) {
      // XXX: can I just add getChildExprs?
      nodes.add(filter.getCondition());
    }
    return nodes;
  }

  private static List<RexInputRef> getAllInputRefs(List<RexNode> nodes) {
    List<RexInputRef> inputRefs = Lists.newArrayList();
    for (RexNode node : nodes) {
      if (node instanceof RexInputRef) {
        inputRefs.add((RexInputRef) node);
      }
      if (node instanceof RexCall) {
        inputRefs.addAll(getAllInputRefs(((RexCall)node).getOperands()));
      }
    }
    return inputRefs;
  }
}
