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

import java.io.Serializable;

import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.impala.thrift.TExecRequest;

// XXX vectorization vectorization = Explain.Vectorization.SUMMARY_PATH
@Explain(displayName = "Impala", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class ImpalaWork implements Serializable {
    private final String query;
    private final TExecRequest execRequest;
    private final FetchTask fetch;
    private final ImpalaContext impalaContext;

    public ImpalaWork(TExecRequest execRequest, String query, FetchTask fetch, ImpalaContext impalaContext) {
        this.execRequest = execRequest;
        this.query = query;
        this.fetch = fetch;
        this.impalaContext = impalaContext;
    }

    @Explain(displayName = "Impala Plan")
    public String getImpalaExplain() { return "\n" + impalaContext.getPlanExecInfo().getExplainString(org.apache.impala.thrift.TExplainLevel.VERBOSE); }
    public TExecRequest getExecRequest() { return execRequest; }
    public String getQuery() {
        return query;
    }
    public FetchTask getFetch() { return fetch; }
}
