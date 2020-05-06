/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.exec.store.splunk;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos;

import java.util.List;
import java.util.Map;

public class SplunkGroupScan extends AbstractGroupScan {

  private final List<SchemaPath> columns;
  private final SplunkScanSpec splunkScanSpec;
  private final Map<String, String> filters;
  //private final ScanStats scanStats;
  private final double filterSelectivity;


  /**
   * Creates a new group scan from the storage plugin.
   */
  public SplunkGroupScan (SplunkScanSpec scanSpec) {
    super("no-user");
    this.splunkScanSpec = scanSpec;
    this.columns = ALL_COLUMNS;
    this.filters = null;
    this.filterSelectivity = 0.0;
    //this.scanStats = computeScanStats();
  }

  /**
   * Copies the group scan during many stages of Calcite operation.
   */
  public SplunkGroupScan(SplunkGroupScan that) {
    super(that);
    this.splunkScanSpec = that.splunkScanSpec;
    this.columns = that.columns;
    this.filters = that.filters;
    this.filterSelectivity = that.filterSelectivity;

    // Calcite makes many copies in the later stage of planning
    // without changing anything. Retain the previous stats.
    //this.scanStats = that.scanStats;
  }

  /**
   * Applies columns. Oddly called multiple times, even when
   * the scan already has columns.
   */
  public SplunkGroupScan(SplunkGroupScan that, List<SchemaPath> columns) {
    super(that);
    this.columns = columns;
    this.splunkScanSpec = that.splunkScanSpec;

    // Oddly called later in planning, after earlier assigning columns,
    // to again assign columns. Retain filters, but compute new stats.
    this.filters = that.filters;
    this.filterSelectivity = that.filterSelectivity;
   //this.scanStats = computeScanStats();
  }

  /**
   * Adds a filter to the scan.
   */
  public SplunkGroupScan(SplunkGroupScan that, Map<String, String> filters,
                       double filterSelectivity) {
    super(that);
    this.columns = that.columns;
    this.splunkScanSpec = that.splunkScanSpec;

    // Applies a filter.
    this.filters = filters;
    this.filterSelectivity = filterSelectivity;
    //this.scanStats = computeScanStats();
  }

  /**
   * Deserialize a group scan. Not called in normal operation. Probably used
   * only if Drill executes a logical plan.
   */
  @JsonCreator
  public SplunkGroupScan(
    @JsonProperty("columns") List<SchemaPath> columns,
    @JsonProperty("splunkScanSpec") SplunkScanSpec splunkScanSpec,
    @JsonProperty("filters") Map<String, String> filters,
    @JsonProperty("filterSelectivity") double selectivity
  ) {
    super("no-user");
    this.columns = columns;
    this.splunkScanSpec = splunkScanSpec;
    this.filters = filters;
    this.filterSelectivity = selectivity;
    //this.scanStats = computeScanStats();
  }


  @Override
  public void applyAssignments(List<CoordinationProtos.DrillbitEndpoint> endpoints) throws PhysicalOperatorSetupException {

  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException {
    return null;
  }

  @Override
  public int getMaxParallelizationWidth() {
    return 0;
  }

  @Override
  public String getDigest() {
    return null;
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    return null;
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    return new SplunkGroupScan(this, columns);
  }
}
