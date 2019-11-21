// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.hadoop.hive.ql.plan.impala;

import java.util.List;

import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.impala.thrift.TDataSink;
import org.apache.impala.thrift.TDataSinkType;
import org.apache.impala.thrift.TExecStats;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TExpr;
import org.apache.impala.thrift.TPlanRootSink;
import org.apache.impala.thrift.TResultSetMetadata;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class PlanRootSink extends DataSink {

  public final PlanNode rootNode_;

  // public final List<RexNode> projectExprs_;

  public final List<FieldSchema> resultSchema_;

  public PlanRootSink(PlanNode rootNode, List<FieldSchema> resultSchema) {
    rootNode_ = rootNode;
    // projectExprs_ = projectExprs;
    resultSchema_ = resultSchema;
  }

  @Override
  public TDataSink getDerivedTDataSink() {
    TDataSink dataSink = new TDataSink();
    dataSink.setType(TDataSinkType.PLAN_ROOT_SINK);
    TPlanRootSink planRootSink = new TPlanRootSink();
    //XXX:
    planRootSink.setResource_profile(ResourceProfile.invalid().toThrift());
    dataSink.setPlan_root_sink(planRootSink);
    //XXX: fill this in
    dataSink.setLabel("");

    if (rootNode_.getOutputExprs() != null) {
      for (RexNode field : rootNode_.getOutputExprs()) {
        TExpr expr = new TExpr();
        Column c = ExprFactory.createExpr(field);
        //XXX: assuming one tuple right now
        assert rootNode_.getTupleDescriptors().size() == 0;
        expr.setNodes(c.getTExprNodeList(rootNode_.getTupleDescriptors().get(0)));
        dataSink.addToOutput_exprs(expr);
      }
    }

    // XXX hardcoded
    TExecStats stats = new TExecStats();
    stats.setCardinality(10);

    dataSink.setEstimated_stats(stats);
    return dataSink;
  }

  @Override
  public String getDerivedExplainString(String prefix, String detailPrefix,
      /*XXX:TQueryOptions queryOptions,*/ TExplainLevel explainLevel) {
    StringBuilder output = new StringBuilder();
    output.append(String.format("%sPLAN-ROOT SINK\n", prefix));
    if (explainLevel.ordinal() >= TExplainLevel.EXTENDED.ordinal()) {
      output.append(detailPrefix + "output exprs: ")
          .append(Joiner.on(", ").join(getExprNames()) + "\n");
      output.append(detailPrefix);
      //XXX:
      output.append(ResourceProfile.invalid().getExplainString());
      output.append("\n");
    }
    return output.toString();
  }

  private List<String> getExprNames() {
    List<String> exprNames = Lists.newArrayList();
    for (FieldSchema fieldSchema : resultSchema_) {
      exprNames.add(fieldSchema.getName());
    }
    return exprNames;
  }

  public TResultSetMetadata getTResultSetMetadata() {
    TResultSetMetadata resultSetMetadata = new TResultSetMetadata();
    //XXX: this isn't right, we want projects, not slot descriptors
    for (FieldSchema fieldSchema : resultSchema_) {
      Column c = new FieldSchemaColumn(fieldSchema);
      resultSetMetadata.addToColumns(c.getTColumn());
    }
    return resultSetMetadata;
  }
}
