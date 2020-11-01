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

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;
import nl.basjes.parse.core.Casts;
import nl.basjes.parse.core.Parser;
import nl.basjes.parse.core.exceptions.DissectionFailure;
import nl.basjes.parse.core.exceptions.InvalidDissectorException;
import nl.basjes.parse.core.exceptions.MissingDissectorsException;
import nl.basjes.parse.httpdlog.HttpdLoglineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HttpdParser {

  private static final Logger logger = LoggerFactory.getLogger(HttpdParser.class);

  public static final String PARSER_WILDCARD = ".*";
  public static final String SAFE_WILDCARD = "_$";
  public static final String SAFE_SEPARATOR = "_";
  public static final String REMAPPING_FLAG = "#";
  public static String RAW_LINE_COL_NAME = "_raw";

  private final Parser<HttpdLogRecord> parser;
  private RowSetLoader rowWriter;
  private SchemaBuilder builder;

  private HttpdLogRecord record;


  public HttpdParser(final String logFormat,
                     final String timestampFormat)
    throws NoSuchMethodException, MissingDissectorsException, InvalidDissectorException {

    Preconditions.checkArgument(logFormat != null && !logFormat.trim().isEmpty(), "logFormat cannot be null or empty");



    this.parser = new HttpdLoglineParser<>(HttpdLogRecord.class, logFormat, timestampFormat);
    if (timestampFormat != null && !timestampFormat.trim().isEmpty()) {
      logger.info("Custom timestamp format has been specified. This is an informational note only as custom timestamps is rather unusual.");
    }
    if (logFormat.contains("\n")) {
      logger.info("Specified logformat is a multiline log format: {}", logFormat);
    }

    setupParser(logFormat);
  }

  public void setRowWriter(RowSetLoader rowWriter) {
    this.rowWriter = rowWriter;
  }


  /**
   * We do not expose the underlying parser or the record which is used to manage the writers.
   *
   * @param line log line to tear apart.
   * @throws DissectionFailure
   * @throws InvalidDissectorException
   * @throws MissingDissectorsException
   */
  public void parse(final String line) throws DissectionFailure, InvalidDissectorException, MissingDissectorsException {
    parser.parse(record, line);
    record.finishRecord();
  }

  /**
   * In order to define a type remapping the format of the field configuration will look like: <br/>
   * HTTP.URI:request.firstline.uri.query.[parameter name] <br/>
   *
   * @param parser    Add type remapping to this parser instance.
   * @param fieldName request.firstline.uri.query.[parameter_name]
   * @param fieldType HTTP.URI, etc..
   */
  private void addTypeRemapping(final Parser<HttpdLogRecord> parser, final String fieldName, final String fieldType) {
    logger.debug("Adding type remapping - fieldName: {}, fieldType: {}", fieldName, fieldType);
    parser.addTypeRemapping(fieldName, fieldType);
  }

  /**
   * The parser deals with dots unlike Drill wanting underscores request_referer. For the sake of simplicity we are
   * going replace the dots. The resultant output field will look like: request.referer.<br>
   * Additionally, wild cards will get replaced with .*
   *
   * @param drillFieldName name to be cleansed.
   * @return
   */
 /*public static String parserFormattedFieldName(String drillFieldName) {

    //The Useragent fields contain a dash which causes potential problems if the field name is not escaped properly
    //This removes the dash
    if (drillFieldName.contains("useragent")) {
      drillFieldName = drillFieldName.replace("useragent", "user-agent");
    }

    String tempFieldName;
    tempFieldName = LOGFIELDS.get(drillFieldName);
    return tempFieldName.replace(SAFE_WILDCARD, PARSER_WILDCARD).replaceAll(SAFE_SEPARATOR, ".").replaceAll("\\.\\.", "_");
  }*/

  /**
   * Drill cannot deal with fields with dots in them like request.referer. For the sake of simplicity we are going
   * ensure the field name is cleansed. The resultant output field will look like: request_referer.<br>
   * Additionally, wild cards will get replaced with _$
   *
   * @param parserFieldName name to be cleansed.
   * @return The field name formatted for Drill
   */
  public static String drillFormattedFieldName(String parserFieldName) {

    //The Useragent fields contain a dash which causes potential problems if the field name is not escaped properly
    //This removes the dash
    /*if (parserFieldName.contains("user-agent")) {
      parserFieldName = parserFieldName.replace("user-agent", "useragent");
    }*/

    if (parserFieldName.contains(":")) {
      String[] fieldPart = parserFieldName.split(":");
      return fieldPart[1].replaceAll("_", "__").replace(PARSER_WILDCARD, SAFE_WILDCARD).replaceAll("\\.", SAFE_SEPARATOR);
    } else {
      return parserFieldName.replaceAll("_", "__").replace(PARSER_WILDCARD, SAFE_WILDCARD).replaceAll("\\.", SAFE_SEPARATOR);
    }
  }

  public TupleMetadata setupParser(String logFormat)
          throws NoSuchMethodException, MissingDissectorsException, InvalidDissectorException {

    SchemaBuilder builder = new SchemaBuilder()
      .addNullable(RAW_LINE_COL_NAME, TypeProtos.MinorType.VARCHAR);

    /*
     * If the user has selected fields, then we will use them to configure the parser because this would be the most
     * efficient way to parse the log.
     */
    Map<String, String> requestedPaths;
    List<String> allParserPaths = parser.getPossiblePaths();

    /*
     * Use all possible paths that the parser has determined from the specified log format.
     */

    requestedPaths = Maps.newHashMap();
    for (final String parserPath : allParserPaths) {
      requestedPaths.put(drillFormattedFieldName(parserPath), parserPath);
    }

    /*
     * By adding the parse target to the dummy instance we activate it for use. Which we can then use to find out which
     * paths cast to which native data types. After we are done figuring this information out, we throw this away
     * because this will be the slowest parsing path possible for the specified format.
     */
    Parser<Object> dummy = new HttpdLoglineParser<>(Object.class, logFormat);
    // TODO Don't we want requested paths here... not the all possible
    dummy.addParseTarget(String.class.getMethod("indexOf", String.class), allParserPaths);
    for (final Map.Entry<String, String> entry : requestedPaths.entrySet()) {
      final EnumSet<Casts> casts;

      /*
       * Check the field specified by the user to see if it is supposed to be remapped.
       */
      if (entry.getValue().startsWith(REMAPPING_FLAG)) {
        /*
         * Because this field is being remapped we need to replace the field name that the parser uses.
         */
        entry.setValue(entry.getValue().substring(REMAPPING_FLAG.length()));

        final String[] pieces = entry.getValue().split(":");
        addTypeRemapping(parser, pieces[1], pieces[0]);
        casts = Casts.STRING_ONLY;
      } else {
        casts = dummy.getCasts(entry.getValue());
      }

      Casts dataType = (Casts) casts.toArray()[casts.size() - 1];

      switch (dataType) {
        case STRING:
          builder.addNullable(entry.getKey(), TypeProtos.MinorType.VARCHAR);
          break;
        case LONG:
          builder.addNullable(entry.getKey(), TypeProtos.MinorType.BIGINT);
          break;
        case DOUBLE:
          builder.addNullable(entry.getKey(), TypeProtos.MinorType.FLOAT8);
          break;
        default:
          logger.error("HTTPD Unsupported data type {} for field {}", dataType.toString(), entry.getKey());
          break;
      }
    }
    return builder.build();
  }
}
