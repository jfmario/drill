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

import com.splunk.JobExportArgs;
import com.splunk.Service;
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.store.easy.json.loader.JsonLoader;
import org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl;
import org.apache.drill.exec.util.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

public class SplunkBatchReader implements ManagedReader<SchemaNegotiator> {

  private static final Logger logger = LoggerFactory.getLogger(SplunkBatchReader.class);
  private final SplunkPluginConfig config;
  private final SplunkSubScan subScan;
  private final List<SchemaPath> projectedColumns;
  private final Service splunkService;
  private SplunkScanSpec subScanSpec;
  private JobExportArgs exportArgs;
  private JsonLoader jsonLoader;



  public SplunkBatchReader(SplunkPluginConfig config, SplunkSubScan subScan) {
    this.config = config;
    this.subScan = subScan;
    this.projectedColumns = subScan.getColumns();
    this.subScanSpec = subScan.getScanSpec();
    SplunkConnection connection = new SplunkConnection(config);
    this.splunkService = connection.connect();
  }

  @Override
  public boolean open(SchemaNegotiator negotiator) {
    CustomErrorContext parentErrorContext = negotiator.parentErrorContext();

    // Build the Schema
    String queryString = buildQueryString();

    InputStream searchResults = splunkService.export(queryString, exportArgs);

    /*StringWriter writer = new StringWriter();
    try {
      IOUtils.copy(searchResults, writer, "UTF-8");
      String results = writer.toString();
      logger.debug(results);
    } catch (Exception e) {

    }*/


    try {
      jsonLoader = new JsonLoaderImpl.JsonLoaderBuilder()
        .resultSetLoader(negotiator.build())
        .standardOptions(negotiator.queryOptions())
        .dataPath("result")
        .errorContext(parentErrorContext)
        .fromStream(searchResults)
        .build();
    } catch (Throwable t) {
      AutoCloseables.closeSilently(searchResults);
      throw t;
    }
    return true;
  }

  @Override
  public boolean next() {
    return jsonLoader.readBatch();
  }

  @Override
  public void close() {
    if (jsonLoader != null) {
      jsonLoader.close();
      jsonLoader = null;
    }
  }

  private String buildQueryString () {
    String query = "search ";

    SplunkQueryBuilder builder = new SplunkQueryBuilder(subScanSpec.getIndexName());

    // Get the index from the table
    exportArgs = new JobExportArgs();

    // Set to normal search mode
    exportArgs.setSearchMode(JobExportArgs.SearchMode.NORMAL);

    // Set output mode to JSON
    exportArgs.setOutputMode(JobExportArgs.OutputMode.JSON);
    exportArgs.setEnableLookups(true);


    // Splunk searches perform best when they are time bound.  This allows the user to set
    // default time boundaries in the config.  These will be overwritten in filter pushdowns
    exportArgs.setEarliestTime("-7d");
    exportArgs.setLatestTime(config.getLatestTime());

    // Pushdown the selected fields for non star queries.
    if (! Utilities.isStarQuery(projectedColumns)) {
      builder.addField(projectedColumns);
    }

    // Add sourcetype if present
    // TODO For testing only
    builder.addSourceType("access_combined_wcookie");

    // Apply filters
    Map<String, String> filters = subScan.getFilters();
    StringBuilder andFilterString = new StringBuilder();

    // Since Splunk treats filters as AND filters by default, they can simply be added to the search string
    if (filters != null) {
      for (Map.Entry filter : filters.entrySet()) {
        andFilterString
          .append(filter.getKey())
          .append("=")
          .append(filter.getValue())
          .append(" ");
      }
    }

    // Apply limits
    if (subScan.getMaxRecords() > 0) {
      builder.addLimit(subScan.getMaxRecords());
    }
    query = builder.build();

    logger.debug("Sending query to Splunk: {}", query);
    return query;
  }
}

