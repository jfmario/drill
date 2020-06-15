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
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.apache.drill.common.AutoCloseables;
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
import org.apache.drill.exec.store.base.filter.ExprNode;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;
import org.apache.drill.shaded.guava.com.google.common.base.Strings;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SplunkBatchReader implements ManagedReader<SchemaNegotiator> {

  private static final Logger logger = LoggerFactory.getLogger(SplunkBatchReader.class);
  private static final List<String> INT_COLS = new ArrayList<>(Arrays.asList("date_hour", "date_mday", "date_minute", "date_second", "date_year", "linecount"));
  private static final List<String> TS_COLS = new ArrayList<>(Arrays.asList("_indextime", "_time"));
  private static final String EARLIEST_TIME_COLUMN = "earliestTime";
  private static final String LATEST_TIME_COLUMN = "latestTime";

  private final SplunkPluginConfig config;
  private final SplunkSubScan subScan;
  private final List<SchemaPath> projectedColumns;
  private final Service splunkService;
  private final SplunkScanSpec subScanSpec;
  private final CsvParserSettings csvSettings;
  private JobExportArgs exportArgs;
  private InputStream searchResults;
  private CsvParser csvReader;
  private String[] firstRow;
  private CustomErrorContext errorContext;

  private List<SplunkColumnWriter> columnWriters;
  private SchemaBuilder builder;
  private RowSetLoader rowWriter;
  private Stopwatch timer;

  /**
   * These are special fields that alter the queries sent to Splunk.
   */
  private enum SPECIAL_FIELDS {

    /**
     * The sourcetype of a query. Specifying the sourcetype can improve query performance.
     */
    sourcetype,
    /**
     * Used to send raw SPL to Splunk
     */
    spl,
    /**
     * The earliest time boundary of a query
     */
    earliestTime,
    /**
     * The latest time bound of a query
     */
    latestTime
  }


  public SplunkBatchReader(SplunkPluginConfig config, SplunkSubScan subScan) {
    this.config = config;
    this.subScan = subScan;
    this.projectedColumns = subScan.getColumns();
    this.subScanSpec = subScan.getScanSpec();
    SplunkConnection connection = new SplunkConnection(config);
    this.splunkService = connection.connect();

    this.csvSettings = new CsvParserSettings();
    csvSettings.setLineSeparatorDetectionEnabled(true);
    csvSettings.setMaxCharsPerColumn(ValueVector.MAX_BUFFER_SIZE);
  }

  @Override
  public boolean open(SchemaNegotiator negotiator) {
    timer = Stopwatch.createUnstarted();
    timer.start();

    this.errorContext = negotiator.parentErrorContext();

    String queryString = buildQueryString();

    logger.debug("Query Sent to Splunk: {}", queryString);
    // Execute the query
    searchResults = splunkService.export(queryString, exportArgs);
    logger.debug("Time to execute query: {} milliseconds", timer.elapsed().getNano() / 100000);

    /*
    Splunk produces poor output from the API.  Of the available choices, CSV was the easiest to deal with.  Unfortunately,
    the data is not consistent, as some fields are quoted, some are not.
    */
    this.csvReader = new CsvParser(csvSettings);
    csvReader.beginParsing(searchResults);

    logger.debug("Time to open input stream: {} milliseconds", timer.elapsed().getNano() / 100000);

    // Build the Schema
    builder = new SchemaBuilder();
    TupleMetadata drillSchema = buildSchema();
    negotiator.tableSchema(drillSchema, true);
    ResultSetLoader resultLoader = negotiator.build();

    // Create ScalarWriters
    rowWriter = resultLoader.writer();
    populateWriterArray();
    timer.stop();
    logger.debug("Completed open function in {} milliseconds", timer.elapsed().getNano() / 100000);
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
    if (searchResults != null) {
      AutoCloseables.closeSilently(searchResults);
      searchResults = null;
    }
  }

  /**
   * Splunk returns the data in CSV format with some fields escaped and some not.  Splunk does
   * not have the concept of datatypes, or at least does not make the metadata available in the API, so
   * the best solution is to provide a list of columns that are known to be a specific data type such as _time,
   * indextime, the various date components etc and map those as the appropriate columns.  Then map everything else as a string.
   */
  private TupleMetadata buildSchema() {

    this.firstRow = csvReader.parseNext();

    // Case for empty dataset
    if (firstRow == null) {
      return builder.buildSchema();
    }

    // Parse the first row
    for (String value : firstRow) {
      if (INT_COLS.contains(value)) {
        builder.addNullable(value, MinorType.INT);
      } else if (TS_COLS.contains(value)) {
        builder.addNullable(value, MinorType.TIMESTAMP);
      } else {
        try {
          builder.addNullable(value, MinorType.VARCHAR);
        } catch (Exception e) {
          logger.warn("Splunk attempted to add duplicate column {}", value);
        }
      }
    }
    logger.debug("Time to build schmea: {} milliseconds", timer.elapsed().getNano() / 100000);
    return builder.buildSchema();
  }

  private void populateWriterArray() {
    // Case for empty result set
    if (firstRow == null || firstRow.length == 0) {
      return;
    }
    columnWriters = new ArrayList<>();

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
    logger.debug("Time to populate writer array: {} milliseconds", timer.elapsed().getNano() / 100000);
  }

  private boolean processRow() {
    String[] nextRow = csvReader.parseNext();
    if (nextRow == null) {
      return false;
    }
    rowWriter.start();
    for (SplunkColumnWriter columnWriter : columnWriters) {
      columnWriter.load(nextRow);
    }
    rowWriter.save();
    return true;
  }

  /**
   * Checks to see whether the query is a star query. For our purposes, the star query is
   * anything that contains only the ** and the SPECIAL_COLUMNS which are not projected.
   * @return true if it is a star query, false if not.
   */
  private boolean isStarQuery() {
    boolean isStarQuery = false;
    List specialFields = Arrays.asList(SPECIAL_FIELDS.values());

    for (SchemaPath path: projectedColumns) {
      if (path.nameEquals("**")) {
        isStarQuery = true;
      } else if (specialFields.contains(path.getAsNamePart())) {
        isStarQuery = true;
      } else {
        return false;
      }
    }
    return isStarQuery;
  }

  private String buildQueryString () {
    String query = "search ";
    String earliestTime = null;
    String latestTime = null;
    Map<String, ExprNode.ColRelOpConstNode> filters = subScan.getFilters();

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
    if (filters != null && filters.containsKey(EARLIEST_TIME_COLUMN)) {
      earliestTime = filters.get(EARLIEST_TIME_COLUMN).value.value.toString();

      // Remove from map
      filters.remove(EARLIEST_TIME_COLUMN);
    }

    if (filters != null && filters.containsKey(LATEST_TIME_COLUMN)) {
      latestTime = filters.get(LATEST_TIME_COLUMN).value.value.toString();

      // Remove from map
      filters.remove(LATEST_TIME_COLUMN);
    }

    if (earliestTime == null) {
      earliestTime = config.getEarliestTime();
    }

    if (latestTime == null) {
      latestTime = config.getLatestTime();
    }

    logger.debug("Query time bounds: {} and {}", earliestTime, latestTime);
    exportArgs.setEarliestTime(earliestTime);
    exportArgs.setLatestTime(latestTime);

    // Special case: If the user wishes to send arbitrary SPL to Splunk, the user can use the "SPL"
    // Index and spl filter
    if (subScanSpec.getIndexName().equalsIgnoreCase("spl")) {
      if (filters == null || filters.get("spl") == null) {
        throw UserException
          .validationError()
          .message("SPL cannot be empty when querying spl table.")
          .addContext(errorContext)
          .build(logger);
      }
      return filters.get("spl").value.value.toString();
    }

    SplunkQueryBuilder builder = new SplunkQueryBuilder(subScanSpec.getIndexName());

    // Set the sourcetype
    if (filters != null && filters.containsKey("sourcetype")) {
      String sourcetype = filters.get("sourcetype").value.value.toString();
      builder.addSourceType(sourcetype);
      filters.remove("sourcetype");
    }

    // Pushdown the selected fields for non star queries.
    if (! isStarQuery()) {
      builder.addField(projectedColumns);
    }

    // Apply filters
    builder.addFilters(filters);

    // Apply limits
    if (subScan.getMaxRecords() > 0) {
      builder.addLimit(subScan.getMaxRecords());
    }
    query = builder.build();
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

    public void load(String[] record) {}
  }

  public static class StringColumnWriter extends SplunkColumnWriter {

    StringColumnWriter(String colName, RowSetLoader rowWriter, int columnIndex) {
      super(colName, rowWriter.scalar(colName), columnIndex);
    }

    @Override
    public void load(String[] record) {
      String value = record[columnIndex];
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
    public void load(String[] record) {
      int value = Integer.parseInt(record[columnIndex]);
      columnWriter.setInt(value);
    }
  }

  /**
   * There are two known time columns in Splunk, the _time and _indextime.
   */
  public static class TimestampColumnWriter extends SplunkColumnWriter {

    TimestampColumnWriter(String colName, RowSetLoader rowWriter, int columnIndex) {
      super(colName, rowWriter.scalar(colName), columnIndex);
    }

    @Override
    public void load(String[] record) {
      long value = Long.parseLong(record[columnIndex]) * 1000;
      columnWriter.setTimestamp(new Instant(value));
    }
  }
}
