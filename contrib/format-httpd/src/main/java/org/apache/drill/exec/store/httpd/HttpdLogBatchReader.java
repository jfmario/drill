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

package org.apache.drill.exec.store.httpd;

import nl.basjes.parse.core.Parser;
import nl.basjes.parse.httpdlog.HttpdLoglineParser;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileSchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.shaded.guava.com.google.common.base.Charsets;
import org.apache.hadoop.mapred.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

public class HttpdLogBatchReader implements ManagedReader<FileSchemaNegotiator> {

  private static final Logger logger = LoggerFactory.getLogger(HttpdLogBatchReader.class);
  public static final String RAW_LINE_COL_NAME = "_raw";
  private final HttpdLogFormatConfig formatConfig;
  private final Parser<HttpdLogRecord> parser;
  private FileSplit split;
  private InputStream fsStream;
  private ResultSetLoader loader;
  private RowSetLoader rowWriter;
  private BufferedReader reader;
  private boolean firstLine = true;
  private int lineNumber;

  public HttpdLogBatchReader(HttpdLogFormatConfig formatConfig) {
    this.formatConfig = formatConfig;
    this.parser = new HttpdLoglineParser<>(HttpdLogRecord.class, formatConfig.getLogFormat(), formatConfig.getTimestampFormat());
  }

  @Override
  public boolean open(FileSchemaNegotiator negotiator) {
    // Open the input stream to the log file
    // TODO Get the error context

    openFile(negotiator);
    loader = negotiator.build();
    rowWriter = loader.writer();
    // Get the parser
    buildSchema();
    return true;
  }

  @Override
  public boolean next() {
    while (! rowWriter.isFull()) {
      if (! nextLine(rowWriter)) {
        return false;
      }
    }
    return true;
  }

  private boolean nextLine(RowSetLoader rowWriter) {
    String line;
    try {
      line = reader.readLine();
    } catch (Exception e) {
      throw UserException
        .dataReadError(e)
        .message("Error reading HTTPD file at line number %d", lineNumber)
        .addContext(e.getMessage())
        .build(logger);
    }

    if (firstLine) {
      firstLine = false;
    }
    // Start the row
    rowWriter.start();


    // Finish the row
    rowWriter.save();
    lineNumber++;

    return false;
  }

  @Override
  public void close() {
    if (fsStream == null) {
      return;
    }
    try {
      fsStream.close();
    } catch (IOException e) {
      logger.warn("Error when closing HTTPD file: {} {}", split.getPath().toString(), e.getMessage());
    }
    fsStream = null;
  }

  private void openFile(FileSchemaNegotiator negotiator) {
    split = negotiator.split();
    try {
      fsStream = negotiator.fileSystem().openPossiblyCompressedStream(split.getPath());
    } catch (Exception e) {
      throw UserException
        .dataReadError(e)
        .message("Failed to open open input file: %s", split.getPath().toString())
        .addContext(e.getMessage())
        .build(logger);
    }
    reader = new BufferedReader(new InputStreamReader(fsStream, Charsets.UTF_8));
  }

  private void buildSchema() {
    logger.debug("Building schema with config {}.", formatConfig.getLogFormat());

    // TODO Make this column not appear in star queries
    SchemaBuilder builder = new SchemaBuilder()
      .addNullable(RAW_LINE_COL_NAME, TypeProtos.MinorType.VARCHAR);

    // Get the possible paths from the log parser
    List<String> possiblePaths = getPossiblePaths();

    String dataType;
    String fieldName;
    String[] pieces;
    for (String fullFieldName : possiblePaths) {
      pieces = fullFieldName.split(":");
      dataType = pieces[0];
      fieldName = pieces[1];

      logger.debug("Fieldname: {}, Data Type: {}", fieldName, dataType);
    }
  }

  private List<String> getPossiblePaths() {
    return parser.getPossiblePaths();
  }

}
