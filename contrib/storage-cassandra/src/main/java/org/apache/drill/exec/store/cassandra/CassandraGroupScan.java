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

package org.apache.drill.exec.store.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.base.filter.ExprNode;
import org.apache.drill.exec.store.cassandra.CassandraSubScan.CassandraSubScanSpec;
import org.apache.drill.exec.store.cassandra.connection.CassandraConnectionManager;
import org.apache.drill.exec.util.Utilities;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;
import org.apache.drill.shaded.guava.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

public class CassandraGroupScan extends AbstractGroupScan implements DrillCassandraConstants{

  private static final Logger logger = LoggerFactory.getLogger(CassandraGroupScan.class);
  private static final Comparator<List<CassandraSubScanSpec>> LIST_SIZE_COMPARATOR = new Comparator<List<CassandraSubScanSpec>>() {
    @Override
    public int compare(List<CassandraSubScanSpec> list1, List<CassandraSubScanSpec> list2) {
      return list1.size() - list2.size();
    }
  };

  private static final Comparator<List<CassandraSubScanSpec>> LIST_SIZE_COMPARATOR_REV = Collections.reverseOrder(LIST_SIZE_COMPARATOR);
  private final CassandraStoragePluginConfig config;
  private final List<SchemaPath> columns;
  private final CassandraScanSpec scanSpec;
  private final Map<String, ExprNode.ColRelOpConstNode> filters;
  private final ScanStats scanStats;
  private final double filterSelectivity;
  private final int maxRecords;
  private CassandraStoragePlugin storagePlugin;
  private Metadata metadata;
  private Cluster cluster;
  private Session session;
  private Map<Integer, List<CassandraSubScanSpec>> endpointFragmentMapping;
  private Set<Host> keyspaceHosts;
  private final Map<String, CassandraPartitionToken> hostTokenMapping = new HashMap<>();
  private int totalAssignmentsTobeDone;
  private ResultSet rs;
  private int hashCode;


  public CassandraGroupScan (CassandraScanSpec scanSpec) {
    super(scanSpec.getConfig().username);
    this.scanSpec = scanSpec;
    this.config = scanSpec.getConfig();
    this.columns = ALL_COLUMNS;
    this.filters = null;
    this.filterSelectivity = 0.0;
    this.maxRecords = -1;
    this.scanStats = computeScanStats();
    init();
  }

  /**
   * Copies the group scan during many stages of Calcite operation.
   */
  public CassandraGroupScan(CassandraGroupScan that) {
    super(that);
    this.config = that.config;
    this.scanSpec = that.scanSpec;
    this.columns = that.columns;
    this.filters = that.filters;
    this.filterSelectivity = that.filterSelectivity;
    this.maxRecords = that.maxRecords;

    // Calcite makes many copies in the later stage of planning
    // without changing anything. Retain the previous stats.
    this.scanStats = that.scanStats;
    init();
  }

  /**
   * Applies columns. Oddly called multiple times, even when
   * the scan already has columns.
   */
  public CassandraGroupScan(CassandraGroupScan that, List<SchemaPath> columns) {
    super(that);
    this.columns = columns;
    this.scanSpec = that.scanSpec;
    this.config = that.config;

    // Oddly called later in planning, after earlier assigning columns,
    // to again assign columns. Retain filters, but compute new stats.
    this.filters = that.filters;
    this.filterSelectivity = that.filterSelectivity;
    this.maxRecords = that.maxRecords;
    this.scanStats = computeScanStats();
  }

  /**
   * Adds a filter to the scan.
   */
  public CassandraGroupScan(CassandraGroupScan that, Map<String, ExprNode.ColRelOpConstNode> filters,
                         double filterSelectivity) {
    super(that);
    this.columns = that.columns;
    this.scanSpec = that.scanSpec;
    this.config = that.config;

    // Applies a filter.
    this.filters = filters;
    this.filterSelectivity = filterSelectivity;
    this.maxRecords = that.maxRecords;
    this.scanStats = computeScanStats();
    init();
  }

  /**
   * Adds a limit to the group scan
   * @param that Previous SplunkGroupScan
   * @param maxRecords the limit pushdown
   */
  public CassandraGroupScan(CassandraGroupScan that, int maxRecords) {
    super(that);
    this.columns = that.columns;
    // Apply the limit
    this.maxRecords = maxRecords;
    this.scanSpec = that.scanSpec;
    this.config = that.config;
    this.filters = that.filters;
    this.filterSelectivity = that.filterSelectivity;
    this.scanStats = computeScanStats();
    init();
  }

  /**
   * Deserialize a group scan. Not called in normal operation. Probably used
   * only if Drill executes a logical plan.
   */
  @JsonCreator
  public CassandraGroupScan(
    @JsonProperty("config") CassandraStoragePluginConfig config,
    @JsonProperty("columns") List<SchemaPath> columns,
    @JsonProperty("splunkScanSpec") CassandraScanSpec splunkScanSpec,
    @JsonProperty("filters") Map<String, ExprNode.ColRelOpConstNode> filters,
    @JsonProperty("filterSelectivity") double selectivity,
    @JsonProperty("maxRecords") int maxRecords,
    @JacksonInject StoragePluginRegistry pluginRegistry
  ) {
    super("no-user");
    this.config = config;
    this.columns = columns;
    this.scanSpec = splunkScanSpec;
    this.filters = filters;
    this.filterSelectivity = selectivity;
    this.maxRecords = maxRecords;
    this.scanStats = computeScanStats();
    this.storagePlugin = pluginRegistry.resolve(config, CassandraStoragePlugin.class);
  }

  @JsonProperty("config")
  public CassandraStoragePluginConfig config() { return config; }

  @JsonProperty("columns")
  public List<SchemaPath> columns() { return columns; }

  @JsonProperty("scanSpec")
  public CassandraScanSpec scanSpec() { return scanSpec; }

  @JsonProperty("filters")
  public Map<String, ExprNode.ColRelOpConstNode> filters() { return filters; }

  @JsonProperty("maxRecords")
  public int maxRecords() { return maxRecords; }

  @JsonProperty("filterSelectivity")
  public double selectivity() { return filterSelectivity; }


  @Override
  public boolean canPushdownProjects(List<SchemaPath> columns) {
    return true;
  }

  @Override
  public boolean supportsLimitPushdown() {
    return true;
  }

  @Override
  public GroupScan applyLimit(int maxRecords) {
    if (maxRecords == this.maxRecords) {
      return null;
    }
    return new CassandraGroupScan(this, maxRecords);
  }

  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  public ScanStats getScanStats() {

    // Since this class is immutable, compute stats once and cache
    // them. If the scan changes (adding columns, adding filters), we
    // get a new scan without cached stats.
    return scanStats;
  }

  private ScanStats computeScanStats() {

    // If this config allows filters, then make the default
    // cost very high to force the planner to choose the version
    // with filters.
    if (allowsFilters() && !hasFilters() && !hasLimit()) {
      return new ScanStats(ScanStats.GroupScanProperty.ESTIMATED_TOTAL_COST,
        1E9, 1E112, 1E12);
    }

    // No good estimates at all, just make up something.
    double estRowCount = 100_000;

    // NOTE this was important! if the predicates don't make the query more
    // efficient they won't get pushed down
    if (hasFilters()) {
      estRowCount *= filterSelectivity;
    }

    if (maxRecords > 0) {
      estRowCount = estRowCount / 2;
    }

    double estColCount = Utilities.isStarQuery(columns) ? DrillScanRel.STAR_COLUMN_COST : columns.size();
    double valueCount = estRowCount * estColCount;
    double cpuCost = valueCount;
    double ioCost = valueCount;

    // Force the caller to use our costs rather than the
    // defaults (which sets IO cost to zero).
    return new ScanStats(ScanStats.GroupScanProperty.ESTIMATED_TOTAL_COST,
      estRowCount, cpuCost, ioCost);
  }

  @JsonIgnore
  public boolean hasFilters() {
    return filters != null;
  }

  @JsonIgnore
  public boolean hasLimit() { return maxRecords == -1; }

  @JsonIgnore
  public boolean allowsFilters() {
    return true;
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    return new CassandraGroupScan(this, columns);
  }

  @Override
  public int getMaxParallelizationWidth() {
    if (storagePlugin.getCluster() == null || storagePlugin.getCluster().isClosed() ) {
      cluster = CassandraConnectionManager.getCluster(config);
    } else {
      cluster = storagePlugin.getCluster();
    }
    return cluster.getMetadata().getAllHosts().size();
  }

  @Override
  @JsonIgnore
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new CassandraGroupScan(this);
  }

  private void init() {
    try {
      logger.debug(String.format("Getting cassandra session from host %s, port: %s.", config.getHosts(), config.getPort()));

      if (storagePlugin.getCluster() == null || storagePlugin.getCluster().isClosed() ) {
        cluster = CassandraConnectionManager.getCluster(config);
        session = cluster.connect();
      } else {
        cluster = storagePlugin.getCluster();
        session = storagePlugin.getSession();
      }

      metadata = session.getCluster().getMetadata();

      Charset charset = StandardCharsets.UTF_8;
      CharsetEncoder encoder = charset.newEncoder();

      keyspaceHosts = session.getCluster().getMetadata().getAllHosts();

      logger.debug("KeySpace hosts for Cassandra : {}", keyspaceHosts);

      if (null == keyspaceHosts) {
        logger.error(String.format("No Keyspace Hosts Found for Cassandra %s:%s .", config.getHosts(), config.getPort()));
        throw new DrillRuntimeException(String.format("No Keyspace Hosts Found for Cassandra %s:%s .", config.getHosts(), config.getPort()));
      }

      String[] tokens = CassandraUtil.getPartitionTokens(metadata.getPartitioner(), keyspaceHosts.size());
      int index = 0;
      for (Host h : keyspaceHosts) {
        CassandraPartitionToken token = new CassandraPartitionToken();
        assert tokens != null;
        token.setLow(tokens[index]);
        if (index + 1 < tokens.length) {
          token.setHigh(tokens[index + 1]);
        }
        hostTokenMapping.put(h.getEndPoint().resolve().getHostName(), token);
        index++;
      }
      logger.debug("Host token mapping: {}", hostTokenMapping);

      Statement q = QueryBuilder.select().all().from(scanSpec.getKeyspace(), scanSpec.getTable());

      if (session.isClosed()) {
        logger.error("Error in initializing CasandraGroupScan. Session Closed.");
        throw new DrillRuntimeException("Error in initializing CasandraGroupScan. Session Closed.");
      }

      rs = session.execute(q);
      totalAssignmentsTobeDone = rs.getAvailableWithoutFetching();

    } catch (Exception e) {
      logger.error("Error in initializing CasandraGroupScan, Error: " + e.getMessage());
      throw new DrillRuntimeException(e);
    }
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> incomingEndpoints) {
    final int numSlots = incomingEndpoints.size();

    Preconditions.checkArgument(numSlots <= totalAssignmentsTobeDone, String.format("Incoming endpoints %d is greater than number of chunks %d", numSlots, totalAssignmentsTobeDone));

    final int minPerEndpointSlot = (int) Math.floor((double) totalAssignmentsTobeDone / numSlots);
    final int maxPerEndpointSlot = (int) Math.ceil((double) totalAssignmentsTobeDone / numSlots);

    /* Map for (index,endpoint)'s */
    endpointFragmentMapping = Maps.newHashMapWithExpectedSize(numSlots);
    /* Reverse mapping for above indexes */
    Map<String, Queue<Integer>> endpointHostIndexListMap = Maps.newHashMap();

    /*
     * Initialize these two maps
     */
    for (int i = 0; i < numSlots; ++i) {
      endpointFragmentMapping.put(i, new ArrayList<>(maxPerEndpointSlot));
      String hostname = incomingEndpoints.get(i).getAddress();
      Queue<Integer> hostIndexQueue = endpointHostIndexListMap.get(hostname);
      if (hostIndexQueue == null) {
        hostIndexQueue = Lists.newLinkedList();
        endpointHostIndexListMap.put(hostname, hostIndexQueue);
      }
      hostIndexQueue.add(i);
    }

    Set<Host> hostsToAssignSet = Sets.newHashSet(keyspaceHosts);

    for (Iterator<Host> hostIterator = hostsToAssignSet.iterator(); hostIterator.hasNext(); /*nothing*/) {
      Host hostEntry = hostIterator.next();

      Queue<Integer> endpointIndexlist = endpointHostIndexListMap.get(hostEntry.getAddress().getHostName());
      if (endpointIndexlist != null) {
        Integer slotIndex = endpointIndexlist.poll();
        List<CassandraSubScanSpec> endpointSlotScanList = endpointFragmentMapping.get(slotIndex);
        endpointSlotScanList.add(hostToSubScanSpec(hostEntry, config.getHosts()));
        // add to the tail of the slot list, to add more later in round robin fashion
        endpointIndexlist.offer(slotIndex);
        // this region has been assigned
        hostIterator.remove();
      }
    }

    /*
     * Build priority queues of slots, with ones which has tasks lesser than 'minPerEndpointSlot' and another which have more.
     */
    PriorityQueue<List<CassandraSubScanSpec>> minHeap = new PriorityQueue<>(numSlots, LIST_SIZE_COMPARATOR);
    PriorityQueue<List<CassandraSubScanSpec>> maxHeap = new PriorityQueue<>(numSlots, LIST_SIZE_COMPARATOR_REV);
    for (List<CassandraSubScanSpec> listOfScan : endpointFragmentMapping.values()) {
      if (listOfScan.size() < minPerEndpointSlot) {
        minHeap.offer(listOfScan);
      } else if (listOfScan.size() > minPerEndpointSlot) {
        maxHeap.offer(listOfScan);
      }
    }

    /*
     * Now, let's process any regions which remain unassigned and assign them to slots with minimum number of assignments.
     */
    if (hostsToAssignSet.size() > 0) {
      for (Host hostEntry : hostsToAssignSet) {
        List<CassandraSubScanSpec> smallestList = minHeap.poll();
        smallestList.add(hostToSubScanSpec(hostEntry, config.getHosts()));
        if (smallestList.size() < maxPerEndpointSlot) {
          minHeap.offer(smallestList);
        }
      }
    }

    /*
     * While there are slots with lesser than 'minPerEndpointSlot' unit work, balance from those with more.
     */
    try {
      // If there is more work left
      if (maxHeap.peek() != null && maxHeap.peek().size() > 0) {
        while (minHeap.peek() != null && minHeap.peek().size() <= minPerEndpointSlot) {
          List<CassandraSubScanSpec> smallestList = minHeap.poll();
          List<CassandraSubScanSpec> largestList = maxHeap.poll();

          smallestList.add(largestList.remove(largestList.size() - 1));
          if (largestList.size() > minPerEndpointSlot) {
            maxHeap.offer(largestList);
          }
          if (smallestList.size() <= minPerEndpointSlot) {
            minHeap.offer(smallestList);
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    /* no slot should be empty at this point */
    assert (minHeap.peek() == null || minHeap.peek().size() > 0) :
      String.format("Unable to assign tasks to some endpoints.\nEndpoints: {}.\nAssignment Map: {}.", incomingEndpoints, endpointFragmentMapping.toString());

    logger.debug("Built assignment map.\nEndpoints: {}.\nAssignment Map: {}", incomingEndpoints, endpointFragmentMapping.toString());

  }

  public CassandraSubScanSpec hostToSubScanSpec(Host host, List<String> contactPoints) {
    CassandraScanSpec spec = scanSpec;
    CassandraPartitionToken token = hostTokenMapping.get(host.getEndPoint().resolve().getHostName());

    return new CassandraSubScanSpec()
      .setTable(spec.getTable())
      .setKeyspace(spec.getKeyspace())
      .setFilter(spec.getFilters())
      .setHosts(contactPoints)
      .setCluster(cluster)
      .setPort(config.getPort())
      .setStartToken(token != null ? token.getLow() : null)
      .setEndToken(token != null ? token.getHigh() : null);
  }

  @Override
  public CassandraSubScan getSpecificScan(int minorFragmentId) {
    assert minorFragmentId < endpointFragmentMapping.size() : String.format("Mappings length [%d] should be greater than minor fragment id [%d] but it isn't.", endpointFragmentMapping.size(), minorFragmentId);
    return new CassandraSubScan(storagePlugin, config, endpointFragmentMapping.get(minorFragmentId), columns, maxRecords, cluster, session);
  }
}
