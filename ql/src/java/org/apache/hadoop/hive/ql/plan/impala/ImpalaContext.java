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

import org.apache.calcite.rel.RelNode;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

import java.util.List;
import java.util.Map;

import org.apache.impala.thrift.TExecRequest;

/**
 * A PlanContext encapsulates the attributes needed to generate the final physical plan
 */
public class ImpalaContext {

  private TExecRequest execRequest;

  private PlanNode rootPlanNode;

  private DescriptorTable descriptorTable;

  private ScanRangeLocations scanRangeLocations;

  private DataSink planRootSink;

  private PlanFragment planFragment;

  private PlanExecInfo planExecInfo;

  private Map<IdGenType, IdGenerator<?>> idGenerators;

  public ImpalaContext() {
    idGenerators = PlanUtils.createIdGenerators();
  }

  public void init(RelNode root, List<FieldSchema> resultSchema, String queryString) {
    if (!(root instanceof PlanNode)) {
      throw new IllegalStateException("Expecting an instance of type PlanNode.");
    }
    try {
      rootPlanNode = (PlanNode) root;
      descriptorTable = PlanUtils.createDescriptorTable(rootPlanNode);
      scanRangeLocations = PlanUtils.createScanRangeLocations(rootPlanNode);
      planRootSink = new PlanRootSink(rootPlanNode, resultSchema);
      IdGenerator<PlanFragmentId> fragmentIdGen = (IdGenerator<PlanFragmentId>) idGenerators.get(IdGenType.FRAGMENT);
      //XXX: fill in display
      planFragment = new PlanFragment(fragmentIdGen.getNextId(), "", rootPlanNode, planRootSink);
      planExecInfo = new PlanExecInfo(planFragment, scanRangeLocations);
      execRequest = ExecRequest.getExecRequest(this, queryString);
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  public Map<IdGenType, IdGenerator<?>>  getIdGenerators() {
    return idGenerators;
  }

  public PlanExecInfo getPlanExecInfo() {
    return planExecInfo;
  }

  public DataSink getDataSink() {
    return planRootSink;
  }

  public ScanRangeLocations getScanRangeLocations() {
    return scanRangeLocations;
  }

  public DescriptorTable getDescriptorTable() {
    return descriptorTable;
  }

  public TExecRequest getExecRequest() {
    return execRequest;
  }

}
