    //XXX: query_id
//    queryCtx.setSession();
    //XXX: queryCtx.setCoord_address();
    //XXX: queryCtx.setCoord_krpc_address();
/*client_request:TClientRequest(stmt:select * from tbl2,
 redacted_stmt:select * from tbl2)*/

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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.impala.thrift.TClientRequest;
import org.apache.impala.thrift.TExecRequest;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.THdfsFileFormat;
import org.apache.impala.thrift.TJoinDistributionMode;
import org.apache.impala.thrift.TKuduReadMode;
import org.apache.impala.thrift.TParquetFallbackSchemaResolution;
import org.apache.impala.thrift.TParquetArrayResolution;
import org.apache.impala.thrift.TParquetTimestampType;
import org.apache.impala.thrift.TPrefetchMode;
import org.apache.impala.thrift.TReplicaPreference;
import org.apache.impala.thrift.TRuntimeFilterMode;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryExecRequest;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TStmtType;
import org.apache.impala.thrift.TTransactionalType;
import org.apache.impala.thrift.TUniqueId;
import org.apache.impala.thrift.TSessionState;
import org.apache.impala.thrift.TSessionType;
import org.apache.impala.thrift.TNetworkAddress;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;

    public class ExecRequest {
  public static TExecRequest getExecRequest(ImpalaContext context, String queryString) {
    TExecRequest execRequest = new TExecRequest();
    execRequest.setStmt_type(TStmtType.QUERY);
    execRequest.setQuery_options(getQueryOptions());
    //XXX: not putting in access events yet
    execRequest.setResult_set_metadata(context.getDataSink().getTResultSetMetadata());
    execRequest.setQuery_exec_request(getQueryExecRequest(context, queryString));
    execRequest.setUser_has_profile_access(true);
    execRequest.setAnalysis_warnings(new ArrayList<>());
    return execRequest;
  }
  
  public static TQueryOptions getQueryOptions() {
    TQueryOptions queryOptions = new TQueryOptions();
    queryOptions.setAbort_on_error(false);
    queryOptions.setMax_errors(100);
    queryOptions.setDisable_codegen(false);
    queryOptions.setBatch_size(0);
    queryOptions.setNum_nodes(1);
    queryOptions.setMax_scan_range_length(0);
    queryOptions.setNum_scanner_threads(0);
    queryOptions.setDebug_action("");
    queryOptions.setMem_limit(0);
    queryOptions.setHbase_caching(0);
    queryOptions.setHbase_cache_blocks(false);
    queryOptions.setParquet_file_size(0);
    //XXX: should we hardcode this now?
    queryOptions.setExplain_level(TExplainLevel.STANDARD);
    queryOptions.setSync_ddl(false);
    queryOptions.setDisable_outermost_topn(false);
    queryOptions.setQuery_timeout_s(0);
    queryOptions.setAppx_count_distinct(false);
    queryOptions.setDisable_unsafe_spills(false);
    queryOptions.setExec_single_node_rows_threshold(100);
    queryOptions.setOptimize_partition_key_scans(false);
    queryOptions.setReplica_preference(TReplicaPreference.CACHE_LOCAL);
    queryOptions.setSchedule_random_replica(false);
    queryOptions.setDisable_streaming_preaggregations(false);
    queryOptions.setRuntime_filter_mode(TRuntimeFilterMode.GLOBAL);
    queryOptions.setRuntime_bloom_filter_size(1048576);
    queryOptions.setRuntime_filter_wait_time_ms(0);
    queryOptions.setDisable_row_runtime_filtering(false);
    queryOptions.setMax_num_runtime_filters(10);
    queryOptions.setParquet_annotate_strings_utf8(false);
    queryOptions.setParquet_fallback_schema_resolution(TParquetFallbackSchemaResolution.POSITION);
    queryOptions.setMt_dop(0);
    queryOptions.setS3_skip_insert_staging(true);
    queryOptions.setRuntime_filter_min_size(1048576);
    queryOptions.setRuntime_filter_max_size(16777216);
    queryOptions.setPrefetch_mode(TPrefetchMode.HT_BUCKET);
    queryOptions.setStrict_mode(false);
    queryOptions.setScratch_limit(-1);
    queryOptions.setEnable_expr_rewrites(true);
    queryOptions.setDecimal_v2(true);
    queryOptions.setParquet_dictionary_filtering(true);
    queryOptions.setParquet_array_resolution(TParquetArrayResolution.THREE_LEVEL);
    queryOptions.setParquet_read_statistics(true);
    queryOptions.setDefault_join_distribution_mode(TJoinDistributionMode.BROADCAST);
    queryOptions.setDisable_codegen_rows_threshold(50000);
    queryOptions.setDefault_spillable_buffer_size(2097152);
    queryOptions.setMin_spillable_buffer_size(65536);
    queryOptions.setMax_row_size(524288);
    queryOptions.setIdle_session_timeout(0);
    queryOptions.setCompute_stats_min_sample_size(1073741824);
    queryOptions.setExec_time_limit_s(0);
    queryOptions.setShuffle_distinct_exprs(true);
    queryOptions.setMax_mem_estimate_for_admission(0);
    queryOptions.setThread_reservation_limit(3000);
    queryOptions.setThread_reservation_aggregate_limit(0);
    queryOptions.setKudu_read_mode(TKuduReadMode.DEFAULT);
    queryOptions.setAllow_erasure_coded_files(false);
    //XXX: not default
    queryOptions.setTimezone("UCT");
    queryOptions.setScan_bytes_limit(0);
    queryOptions.setCpu_limit_s(0);
    queryOptions.setTopn_bytes_limit(536870912);
    //XXX: not default
    queryOptions.setClient_identifier("Hive test, need something better here");
    queryOptions.setResource_trace_ratio(0.0);
    queryOptions.setNum_remote_executor_candidates(3);
    queryOptions.setNum_rows_produced_limit(0);
    queryOptions.setPlanner_testcase_mode(false);
    queryOptions.setDefault_file_format(THdfsFileFormat.TEXT);
    queryOptions.setParquet_timestamp_type(TParquetTimestampType.INT96_NANOS);
    queryOptions.setParquet_read_page_index(true);
    queryOptions.setParquet_write_page_index(true);
    queryOptions.setDisable_hdfs_num_rows_estimate(false);
    queryOptions.setSpool_query_results(false);
    queryOptions.setDefault_transactional_type(TTransactionalType.NONE);
    queryOptions.setStatement_expression_limit(250000);
    queryOptions.setMax_statement_length_bytes(16777216);
    queryOptions.setDisable_data_cache(false);
    queryOptions.setMax_result_spooling_mem(104857600);
    queryOptions.setMax_spilled_result_spooling_mem(1073741824);
    queryOptions.setDisable_hbase_num_rows_estimate(false);
    queryOptions.setFetch_rows_timeout_ms(10000);
    queryOptions.setNow_string("");
    queryOptions.setParquet_object_store_split_size(268435456);
    queryOptions.setMem_limit_executors(0);
    return queryOptions;
  }
  
  public static TQueryExecRequest getQueryExecRequest(ImpalaContext context, String queryString) {
    TQueryExecRequest queryExecRequest = new TQueryExecRequest();
    queryExecRequest.setPlan_exec_info(ImmutableList.of(context.getPlanExecInfo().toThrift()));
    queryExecRequest.setQuery_ctx(getQueryCtx(context, queryString));
    //XXX: query_id
    queryExecRequest.setQuery_plan(context.getPlanExecInfo().getExplainString(TExplainLevel.VERBOSE));
    queryExecRequest.setStmt_type(TStmtType.QUERY);
    queryExecRequest.setHost_list(context.getScanRangeLocations().getHostIndexes());
    queryExecRequest.setPer_host_mem_estimate(33554432);
    queryExecRequest.setMax_per_host_min_mem_reservation(8192);
    queryExecRequest.setMax_per_host_thread_reservation(2);
    queryExecRequest.setDedicated_coord_mem_estimate(138412032);
    return queryExecRequest;
  }

  public static TQueryCtx getQueryCtx(ImpalaContext context, String queryString) {
    TQueryCtx queryCtx = new TQueryCtx();
    queryCtx.setClient_request(getClientRequest(queryString));
    //XXX:
//    queryCtx.setSession();
    /*session:TSessionState(session_id:TUniqueId(hi:-1347751261530967742, lo:-7952852071358192240), session_type:BEESWAX, database:sjc_db, connected_user:vagrant, network_address:TNetworkAddress(hostname:::1, port:35318), kudu_latest_observed_ts:0)*/
    //XXX
    queryCtx.setNow_string("2019-11-08 02:05:29.578009");
    //XXX: 
    queryCtx.setPid(9212);
    //XXX: queryCtx.setCoord_address();
    //XXX: queryCtx.setCoord_krpc_address();

    TUniqueId uniqueId = new TUniqueId();

    org.apache.impala.thrift.TSessionState sessionState = new TSessionState();
    sessionState.setConnected_user("hiveServer-2");
    sessionState.setDatabase("default");
    sessionState.setSession_id(uniqueId);
    sessionState.setSession_type(TSessionType.HIVESERVER2);

    TNetworkAddress addr = new TNetworkAddress();
    addr.setHostname("localhost");
    addr.setPort(1984);

    sessionState.setNetwork_address(addr);

    queryCtx.setSession(sessionState);
    //uniqueId.setHi(0xBEEF);
    //uniqueId.setLo(0xF00F);
    queryCtx.setQuery_id(uniqueId);
    queryCtx.setTables_missing_stats(context.getDescriptorTable().getTablesMissingStats());
    queryCtx.setDisable_spilling(false);
    queryCtx.setSnapshot_timestamp(-1);
    queryCtx.setDesc_tbl_serialized(context.getDescriptorTable().toSerializedThrift());
    queryCtx.setStart_unix_millis(1573178729578L);
    queryCtx.setDisable_codegen_hint(false);
    queryCtx.setRequest_pool("default-pool");
    queryCtx.setUtc_timestamp_string("2019-11-08 02:05:29.578009");
    queryCtx.setLocal_time_zone("UCT");
    queryCtx.setDisable_hbase_num_rows_estimate(false);
    queryCtx.setTrace_resource_usage(false);
    queryCtx.setStatus_report_interval_ms(5000);
    queryCtx.setStatus_report_max_retry_s(600);

    // XXX: hardcoded
    TNetworkAddress cordAddr = new TNetworkAddress();
    cordAddr.setHostname("127.0.0.1");
    cordAddr.setPort(22000);
    queryCtx.setCoord_address(cordAddr);

    // XXX: hardcoded
    TNetworkAddress krpcCordAddr = new TNetworkAddress();

    // XXX: Remove HiveConf when cleaned up
    String krpcHostIP = SessionState.get().getConf().getVar(HiveConf.ConfVars.IMPALA_KRPC_ADDRESS);
    krpcCordAddr.setHostname(krpcHostIP);
    krpcCordAddr.setPort(27000);
    queryCtx.setCoord_krpc_address(krpcCordAddr);

    return queryCtx;
  }

  public static TClientRequest getClientRequest(String queryString) {
    TClientRequest clientRequest = new TClientRequest();
//XXX:
/*client_request:TClientRequest(stmt:select * from tbl2,
 redacted_stmt:select * from tbl2)*/

    clientRequest.setStmt(queryString);
    clientRequest.setQuery_options(getQueryOptions());
    return clientRequest;
  }

  private long convertByteArrayToLong(byte[] byteArray, int startIdx) {
    long value = 0;
    for (int i = startIdx; i < startIdx + 8; i++) {
      value = (value << 8) + (byteArray[i] & 0xff);
    }
    return value;
  }
}
