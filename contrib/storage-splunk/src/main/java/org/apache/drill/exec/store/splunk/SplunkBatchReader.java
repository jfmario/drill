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
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.util.Utilities;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.shaded.guava.com.google.common.base.Strings;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SplunkBatchReader implements ManagedReader<SchemaNegotiator> {

  private static final Logger logger = LoggerFactory.getLogger(SplunkBatchReader.class);
  private static final List<String> INT_COLS = new ArrayList<>(Arrays.asList("date_hour", "date_mday", "date_minute", "date_second", "date_year", "linecount"));
  private static final List<String> TS_COLS = new ArrayList<>(Arrays.asList("_indextime", "_time"));

  private final SplunkPluginConfig config;
  private final SplunkSubScan subScan;
  private final List<SchemaPath> projectedColumns;
  private final Service splunkService;
  private final SplunkScanSpec subScanSpec;
  private JobExportArgs exportArgs;
  private Iterator<CSVRecord> csvIterator;
  private CustomErrorContext errorContext;

  private List<SplunkColumnWriter> columnWriters;
  private CSVRecord firstRow;
  private SchemaBuilder builder;
  private RowSetLoader rowWriter;


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
    this.errorContext = negotiator.parentErrorContext();

    String queryString = buildQueryString();

    // Execute the query
    InputStream searchResults = splunkService.export(queryString, exportArgs);

    /*
    Splunk produces poor output from the API.  Of the available choices, CSV was the easiest to deal with.  Unfortunately,
    the data is not consistent, as some fields are quoted, some are not.
    */
    try {
      BufferedReader br = new BufferedReader(new InputStreamReader(searchResults, StandardCharsets.UTF_8));
      this.csvIterator = CSVFormat.DEFAULT.parse(br).iterator();
    } catch (IOException e) {
      throw UserException
        .dataReadError(e)
        .message("Error reading data from Splunk")
        .addContext(errorContext)
        .build(logger);
    }

    // Build the Schema
    builder = new SchemaBuilder();
    TupleMetadata drillSchema = buildSchema();
    negotiator.tableSchema(drillSchema, true);
    ResultSetLoader resultLoader = negotiator.build();

    // Create ScalarWriters
    rowWriter = resultLoader.writer();
    populateWriterArray();

    return true;
  }

  @Override
  public boolean next() {
    while (!rowWriter.isFull()) {
      if (!processRow()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void close() {

  }

  /**
   * Splunk returns the data in CSV format with some fields escaped and some not.  Splunk does
   * not have the concept of datatypes, or at least does not make the metadata available in the API, so
   * the best solution is to provide a list of columns that are known to be a specific data type such as _time,
   * indextime, the various date components etc and map those as the appropriate columns.  Then map everything else as a string.
   */
  private TupleMetadata buildSchema() {
    // Case for empty dataset
    if (!csvIterator.hasNext()) {
      return builder.buildSchema();
    }

    // Get the first row
    firstRow = csvIterator.next();
    for (String value : firstRow) {
      if (INT_COLS.contains(value)) {
        builder.addNullable(value, MinorType.INT);
      } else if (TS_COLS.contains(value)) {
        builder.addNullable(value, MinorType.TIMESTAMP);
      } else {
        builder.addNullable(value, MinorType.VARCHAR);
      }
    }

    return builder.buildSchema();
  }

  private void populateWriterArray() {
    columnWriters = new ArrayList<>();

    // Case for empty result set
    if (firstRow == null || firstRow.size() == 0) {
      return;
    }

    int colPosition = 0;
    for (String value : firstRow) {
      if (INT_COLS.contains(value)) {
        columnWriters.add(new IntColumnWriter(value, rowWriter, colPosition));
      } else if (TS_COLS.contains(value)) {
        columnWriters.add(new TimestampColumnWriter(value, rowWriter, colPosition));
      } else {
        columnWriters.add(new StringColumnWriter(value, rowWriter, colPosition));
      }
      colPosition++;
    }
  }

  private boolean processRow() {
    if (! csvIterator.hasNext()) {
      return false;
    }
    CSVRecord record = csvIterator.next();

    rowWriter.start();
    for (SplunkColumnWriter columnWriter : columnWriters) {
      columnWriter.load(record);
    }
    rowWriter.save();
    return true;
  }

  private String buildQueryString () {
    String query = "search ";

    SplunkQueryBuilder builder = new SplunkQueryBuilder(subScanSpec.getIndexName());

    // Get the index from the table
    exportArgs = new JobExportArgs();

    // Set to normal search mode
    exportArgs.setSearchMode(JobExportArgs.SearchMode.NORMAL);

    // Set all time stamps to epoch seconds
    exportArgs.setTimeFormat("%s");

    // Set output mode to CSV
    exportArgs.setOutputMode(JobExportArgs.OutputMode.CSV);
    exportArgs.setEnableLookups(true);


    // Splunk searches perform best when they are time bound.  This allows the user to set
    // default time boundaries in the config.  These will be overwritten in filter pushdowns
    // TODO Check to see if the earliest and latest time are included in the filters
    exportArgs.setEarliestTime(config.getEarliestTime());
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

  public abstract static class SplunkColumnWriter {

    final String colName;
    ScalarWriter columnWriter;
    int columnIndex;

    public SplunkColumnWriter(String colName, ScalarWriter writer, int columnIndex) {
      this.colName = colName;
      this.columnWriter = writer;
      this.columnIndex = columnIndex;
    }

    public void load(CSVRecord record) {}
  }

  public static class StringColumnWriter extends SplunkColumnWriter {

    StringColumnWriter(String colName, RowSetLoader rowWriter, int columnIndex) {
      super(colName, rowWriter.scalar(colName), columnIndex);
    }

    @Override
    public void load(CSVRecord record) {
      String value = record.get(columnIndex);
      if (Strings.isNullOrEmpty(value)) {
        columnWriter.setNull();
      } else {
        columnWriter.setString(value);
      }
    }
  }

  public static class IntColumnWriter extends SplunkColumnWriter {

    IntColumnWriter(String colName, RowSetLoader rowWriter, int columnIndex) {
      super(colName, rowWriter.scalar(colName), columnIndex);
    }

    @Override
    public void load(CSVRecord record) {
      int value = Integer.parseInt(record.get(columnIndex));
      columnWriter.setInt(value);
    }
  }

  /**
   * There are two known time columns in Splunk, the _time and _indextime.  As Splunk would have it,
   * they are returned in different formats.
   */
  public static class TimestampColumnWriter extends SplunkColumnWriter {

    TimestampColumnWriter(String colName, RowSetLoader rowWriter, int columnIndex) {
      super(colName, rowWriter.scalar(colName), columnIndex);
    }

    @Override
    public void load(CSVRecord record) {
      long value = Long.parseLong(record.get(columnIndex)) * 1000;
      columnWriter.setTimestamp(new Instant(value));
    }
  }
}
