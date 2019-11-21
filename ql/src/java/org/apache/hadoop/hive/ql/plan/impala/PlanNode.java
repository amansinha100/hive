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

package org.apache.hadoop.hive.ql.plan.impala;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.impala.thrift.TExecNodePhase;
import org.apache.impala.thrift.TExecStats;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TExpr;
import org.apache.impala.thrift.TExprNode;
import org.apache.impala.thrift.TPlan;
import org.apache.impala.thrift.TPlanNode;
import org.apache.impala.thrift.TPlanRootSink;
import org.apache.impala.thrift.TResultSetMetadata;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PlanNode extends AbstractRelNode {
  protected abstract TPlanNode createDerivedTPlanNode();

  protected abstract String getDerivedExplainString(String rootPrefix, String detailPrefix, TExplainLevel detailLevel);

  private final PlanId id_;

  private final String displayName_;

  private final ImmutableList<TupleDescriptor> tuples_;

  private final ResourceProfile nodeResourceProfile_;

  private final ImmutableList<PipelineMembership> pipelines_;

  private final HiveFilter filter_;

  //XXX:
  private final int limit_ = -1;

  //XXX:
  private final int cardinality_ = 0;

  //XXX:
  private final int avgRowSize_ = 0;

  private List<RexNode>  outputExprs;


  public PlanNode(RelOptCluster cluster, RelTraitSet traitSet, List<TupleDescriptor> tuples, HiveFilter filter, PlanId planId, String displayName) {
    super(cluster, traitSet);
    id_ = planId;
    displayName_ = displayName;
    tuples_ = new ImmutableList.Builder<TupleDescriptor>().addAll(tuples).build();
    nodeResourceProfile_ = new ResourceProfile(true, 1024*1024, 1024*1024, 1024*1024*8, -1, 1024*1024*8, 1);
    //XXX: when we have children, this will need to change
    pipelines_ = new ImmutableList.Builder<PipelineMembership>().add(
        new PipelineMembership(id_, 0, TExecNodePhase.GETNEXT)).build();
    filter_ = filter;
  }

  public void setOutputExprs(List<RexNode> outputExprs) {
    this.outputExprs = outputExprs;
  }

  public List<RexNode> getOutputExprs() {
    return outputExprs;
  }

  // Convert this plan node, including all children, to its Thrift representation.
  public TPlan treeToThrift() {
    TPlan result = new TPlan();
    result.setNodes(getTPlanNodes());
    return result;
  }

  public List<TPlanNode> getTPlanNodes() {
    List<TPlanNode> planNodes = Lists.newArrayList();
    planNodes.add(getTPlanNode());
    //XXX: don't call children for exchange?
    for (RelNode input : getInputs()) {
      assert input instanceof PlanNode;
      planNodes.addAll(((PlanNode)input).getTPlanNodes());
    }
    return planNodes;
  }

  public TPlanNode getTPlanNode() {
    TPlanNode planNode = createDerivedTPlanNode();
    planNode.setNode_id(id_.asInt());
    planNode.setLimit(-1);

    TExecStats estimatedStats = new TExecStats();
    //TODO: get stats
    estimatedStats.setCardinality(1);
    estimatedStats.setMemory_used(16*1024);
    planNode.setEstimated_stats(estimatedStats);

    planNode.setLabel(getDisplayLabel());
    planNode.setLabel_detail(getDisplayLabelDetail());

    Preconditions.checkState(tuples_.size() > 0); 
    planNode.setRow_tuples(Lists.<Integer>newArrayListWithCapacity(tuples_.size()));
    planNode.setNullable_tuples(Lists.<Boolean>newArrayListWithCapacity(tuples_.size()));
    for (TupleDescriptor tuple : tuples_) {
      planNode.addToRow_tuples(tuple.getTupleId());
      planNode.addToNullable_tuples(tuple.isNullable());
    }   
    
    planNode.setConjuncts(getConjuncts());
    /*
    for (Expr e: conjuncts_) {
      planNode.addToConjuncts(e.treeToThrift());
    }   
    // Serialize any runtime filters
    for (RuntimeFilter filter : runtimeFilters_) {
      planNode.addToRuntime_filters(filter.toThrift());
    }   
    */
    planNode.setDisable_codegen(false);

    Preconditions.checkState(nodeResourceProfile_.isValid());
    planNode.setResource_profile(nodeResourceProfile_.toThrift());
    planNode.setPipelines(Lists.newArrayList());
    for (PipelineMembership pipe : pipelines_) {
      planNode.addToPipelines(pipe.toThrift());
    }   
    return planNode;
  }

  public List<TupleDescriptor> getTupleDescriptors() {
    return tuples_;
  }

  public List<TupleDescriptor> gatherAllTupleDescriptors() {
    //XXX: no children yet, can just return these descriptors
    return tuples_;
  }

  public List<TableDescriptor> gatherAllTableDescriptors() {
    //XXX: no children yet, can just return these descriptors
    List<TableDescriptor> tableDescriptors = Lists.newArrayList();
    for (TupleDescriptor tuple : tuples_) {
      tableDescriptors.add(tuple.getTableDescriptor());
    }
    return tableDescriptors;
  }

  public List<ScanNode> gatherAllScanNodes() {
    //XXX: no children yet, can just return this if it is a scan node 
    List<ScanNode> scanNodes = Lists.newArrayList();
    if (this instanceof ScanNode) {
      scanNodes.add((ScanNode) this);
    }
    return scanNodes;
  }

  public List<SlotDescriptor> gatherAllSlotDescriptors() {
    //XXX: no children yet, can just return these descriptors
    List<SlotDescriptor> slotDescriptors = Lists.newArrayList();
    for (TupleDescriptor tuple : tuples_) {
      slotDescriptors.addAll(tuple.getSlotDescriptors());
    }
    return slotDescriptors;
  }

  protected String getDisplayLabel() {
    return String.format("%s:%s", id_.toString(), displayName_);
  } 
      
  /** 
   * Subclasses can override to provide a node specific detail string that
   * is displayed to the user.
   * e.g. scan can return the table name.
   */   
  protected String getDisplayLabelDetail() { return ""; }

  //XXX: didn't populate stats yet
  public boolean isTableMissingStats() {
    return false;
  }

  public boolean hasCorruptTableStats() {
    return false;
  }

  public List<SlotDescriptor> getSlotDescriptors() {
    List<SlotDescriptor> result = Lists.newArrayList();
    for (TupleDescriptor tuple : tuples_) {
      result.addAll(tuple.getSlotDescriptors());
    }
    return result;
  }

  public PlanId getPlanId() {
    return id_;
  }

  public String getExplainString(String rootPrefix, String prefix,
      /*TQueryOptions queryOptions,*/ TExplainLevel detailLevel) {
    StringBuilder expBuilder = new StringBuilder();
    String detailPrefix = prefix;
    String filler;
    boolean printFiller = (detailLevel.ordinal() >= TExplainLevel.STANDARD.ordinal());

    // Do not traverse into the children of an Exchange node to avoid crossing
    // fragment boundaries.
    //XXX: need to handle inputs
/*
    boolean traverseChildren = !children_.isEmpty() &&
        !(this instanceof ExchangeNode && detailLevel == TExplainLevel.VERBOSE);
*/
    boolean traverseChildren = false;

    if (traverseChildren) {
      detailPrefix += "|  ";
      filler = prefix + "|";
    } else {
      detailPrefix += "   ";
      filler = prefix;
    }  

    // Print the current node
    // The plan node header line will be prefixed by rootPrefix and the remaining details
    // will be prefixed by detailPrefix.
    expBuilder.append(getDerivedExplainString(rootPrefix, detailPrefix, detailLevel));

/*XXX
    if (detailLevel.ordinal() >= TExplainLevel.STANDARD.ordinal() &&
        !(this instanceof SortNode)) {
      if (limit_ != -1) expBuilder.append(detailPrefix + "limit: " + limit_ + "\n");
      expBuilder.append(getOffsetExplainString(detailPrefix));
    }
*/

    boolean displayCardinality = displayCardinality(detailLevel);
    if (detailLevel.ordinal() >= TExplainLevel.EXTENDED.ordinal()) {
      // Print resource profile.
      expBuilder.append(detailPrefix);
      expBuilder.append(nodeResourceProfile_.getExplainString());
      expBuilder.append("\n");
  
      // Print tuple ids, row size and cardinality.
      expBuilder.append(detailPrefix + "tuple-ids=");
      for (int i = 0; i < tuples_.size(); ++i) {
        TupleDescriptor tuple = tuples_.get(i);
        String nullIndicator = tuple.isNullable() ? "N" : "";
        expBuilder.append(tuple.getTupleId() + nullIndicator);
        if (i + 1 != tuples_.size()) expBuilder.append(",");
      }
      expBuilder.append(displayCardinality ? " " : "\n");
    }
    // Output cardinality: in standard and above levels.
    // In standard, on a line by itself (if wanted). In extended, on
    // a line with tuple ids.
    if (displayCardinality) {
      if (detailLevel == TExplainLevel.STANDARD) expBuilder.append(detailPrefix);
      //XXX:
      expBuilder.append("row-size=")
        .append(PrintUtils.printBytes(Math.round(avgRowSize_)))
        .append(" cardinality=")
        .append(PrintUtils.printEstCardinality(cardinality_))
        .append("\n");
    }

    if (detailLevel.ordinal() >= TExplainLevel.EXTENDED.ordinal()) {
      expBuilder.append(detailPrefix);
      expBuilder.append("in pipelines: ");
      if (pipelines_ != null) {
        List<String> pipelines = Lists.newArrayList();
        for (PipelineMembership pipe: pipelines_) {
          pipelines.add(pipe.getExplainString());
        }
        if (pipelines.isEmpty()) expBuilder.append("<none>");
        else expBuilder.append(Joiner.on(", ").join(pipelines));
        expBuilder.append("\n");
      } else {
        expBuilder.append("<not computed>");
      }
    }

    // Print the children. Do not traverse into the children of an Exchange node to
    // avoid crossing fragment boundaries.
    //XXX: no children yet
/*
    if (traverseChildren) {
      if (printFiller) expBuilder.append(filler + "\n");
      String childHeadlinePrefix = prefix + "|--";
      String childDetailPrefix = prefix + "|  ";
      for (int i = children_.size() - 1; i >= 1; --i) {
        PlanNode child = getChild(i);
        if (fragment_ != child.fragment_) {
          // we're crossing a fragment boundary
          expBuilder.append(
              child.fragment_.getExplainString(
                childHeadlinePrefix, childDetailPrefix, queryOptions, detailLevel));
        } else {
          expBuilder.append(child.getExplainString(childHeadlinePrefix,
              childDetailPrefix, queryOptions, detailLevel));
        }
        if (printFiller) expBuilder.append(filler + "\n");
      }
      PlanFragment childFragment = children_.get(0).fragment_;
      if (fragment_ != childFragment && detailLevel == TExplainLevel.EXTENDED) {
        // we're crossing a fragment boundary - print the fragment header.
        expBuilder.append(childFragment.getFragmentHeaderString(prefix, prefix,
            queryOptions.getMt_dop()));
      }
      expBuilder.append(
          children_.get(0).getExplainString(prefix, prefix, queryOptions, detailLevel));
    }
*/

    // Output cardinality, cost estimates and tuple Ids only when explain plan level
    // is extended or above.

    return expBuilder.toString();
  }
    
  //XXX:
  /** 
   * Per-node setting whether to include cardinality in the node overview.
   * Some nodes omit cardinality because either a) it is not needed
   * (Empty set, Exchange), or b) it is printed by the node itself (HDFS scan.)
   * @return true if cardinality should be included in the generic
   * node details, false if it should be omitted.
   */ 
  protected boolean displayCardinality(TExplainLevel detailLevel) {
    return detailLevel.ordinal() >= TExplainLevel.STANDARD.ordinal();
  }     
      
  protected String getOffsetExplainString(String prefix) {
    return "";
  }

  private List<TExpr> getConjuncts() {
    if (filter_ == null) {
      return ImmutableList.of();
    }
    //XXX: only handles 1 level of expression (no ands yet)
    TExpr expr = new TExpr();
    Column filterColumn = ExprFactory.createExpr(filter_.getCondition());
    //XXX: this is going to be a problem later
    assert tuples_.size() == 1;
    expr.setNodes(filterColumn.getTExprNodeList(tuples_.get(0)));
    return ImmutableList.of(expr);
  } 
}
