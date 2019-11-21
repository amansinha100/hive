/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hive.ql.parse;

import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.impala.ImpalaWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

public class ImpalaCompiler extends TaskCompiler {
    protected static final Logger LOG = LoggerFactory.getLogger(ImpalaCompiler.class);

    ImpalaCompiler() { }

    @Override
    protected void decideExecMode(List<Task<?>> rootTasks, Context ctx, GlobalLimitCtx globalLimitCtx) throws SemanticException {
        // does nothing
        LOG.info("decideExecMode");
    }

    @Override
    protected void optimizeTaskPlan(List<Task<?>> rootTasks, ParseContext pCtx, Context ctx) throws SemanticException {
        LOG.info("optimizeTaskPlan");

    }

    @Override
    protected void setInputFormat(Task<?> rootTask) {
        LOG.info("setInput");
    }

    @Override
    protected void generateTaskTree(List<Task<?>> rootTasks, ParseContext pCtx,
                                    List<Task<MoveWork>> mvTask, Set<ReadEntity> inputs, Set<WriteEntity> outputs)
            throws SemanticException {
        LOG.info("generateTaskTree");
        rootTasks.add(TaskFactory.get(new ImpalaWork(pCtx.getContext().getImpalaContext().getExecRequest(),
                pCtx.getQueryState().getQueryString(), pCtx.getFetchTask(), pCtx.getContext().getImpalaContext())));
    }

    @Override
    protected void optimizeOperatorPlan(ParseContext pCtx, Set<ReadEntity> inputs,
                                        Set<WriteEntity> outputs) throws SemanticException {
        LOG.info("optimizerOperatorPlan");
    }

}
