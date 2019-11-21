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

import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

public class PlanUtils {
    public static DescriptorTable createDescriptorTable(PlanNode rootPlanNode) {
        List<TupleDescriptor> tupleDescriptors = rootPlanNode.gatherAllTupleDescriptors();
        List<SlotDescriptor> slotDescriptors = rootPlanNode.gatherAllSlotDescriptors();
        List<TableDescriptor> tableDescriptors = rootPlanNode.gatherAllTableDescriptors();
        return new DescriptorTable(tupleDescriptors, slotDescriptors, tableDescriptors);
    }

    public static ScanRangeLocations createScanRangeLocations(PlanNode rootPlanNode) {
        return new ScanRangeLocations(rootPlanNode.gatherAllScanNodes());
    }

    public static Map<IdGenType, IdGenerator<?>> createIdGenerators() {
        Map<IdGenType, IdGenerator<?>> idGenerators = Maps.newHashMap();
        idGenerators.put(IdGenType.SLOT, SlotId.createGenerator());
        idGenerators.put(IdGenType.PLAN, PlanId.createGenerator());
        idGenerators.put(IdGenType.TUPLE, TupleId.createGenerator());
        idGenerators.put(IdGenType.TABLE, TableId.createGenerator());
        idGenerators.put(IdGenType.FRAGMENT, PlanFragmentId.createGenerator());
        return idGenerators;
    }
}
