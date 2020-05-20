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

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.shaded.guava.com.google.common.base.Strings;

import java.util.List;

public class SplunkQueryBuilder {
  private final String index;
  private String query;
  private String sourceTypes;
  private String fieldList;
  private int limit;

  public SplunkQueryBuilder (String index) {
    this.index = index;
    query = "search index=" + this.index;
  }

  public SplunkQueryBuilder addSourceType(String sourceType) {
    if (this.sourceTypes == null) {
      this.sourceTypes = " sourcetype=\"" + sourceType + "\"";
    } else {
      this.sourceTypes += " OR sourcetype=\"" + sourceType + "\"";
    }
    return this;
  }

  /**
   * Adds a field name to a Splunk query.  To push down the projection into Splunk,
   * Splunk accepts arguments in the format | fields foo, bar, car.  This function adds these fields to the query.
   * As an error preventative measure, this function will ignore ** from Drill.
   * @param field The field to be added to the query
   * @return SplunkQueryBuilder with the completed field string
   */
  public SplunkQueryBuilder addField (String field) {
    // Double Star fields cause errors and we will not add to the field list
    if (field.equalsIgnoreCase("**") || Strings.isNullOrEmpty(field)) {
      return this;
    }

    // Case for first field
    if (fieldList == null) {
      this.fieldList = field;
    } else {
      this.fieldList += "," + field;
    }
    return this;
  }

  /**
   * Creates the field list of r
   * As an error preventative measure, this function will ignore ** from Drill.
   * @param columnList SchemaPath of columns to be added to the field list
   * @return builder with the field added
   */
  public SplunkQueryBuilder addField (List<SchemaPath> columnList) {
    for (SchemaPath column : columnList) {
      String columnName = column.getAsUnescapedPath();
      if (columnName.equalsIgnoreCase("**") || Strings.isNullOrEmpty(columnName)) {
        continue;
      } else {
        addField(columnName);
      }
    }
    return this;
  }

  /**
   * Adds a row limit to the query. Ignores anything <= zero.
   * @param limit Positive, non-zero integer of number of desired rows.
   * @return
   */
  public SplunkQueryBuilder addLimit(int limit) {
    if (limit > 0) {
      this.limit = limit;
    }
    return this;
  }


  public String build() {
    // Add the sourcetype
    if (sourceTypes != null) {
      query += sourceTypes;
    }

    // Add fields
    if (! Strings.isNullOrEmpty(fieldList)) {
      query += " | fields " + fieldList;
    }

    // Add table logic. This tells Splunk to return the data in tabular form rather than the mess that it usually generates
    if ( Strings.isNullOrEmpty(fieldList)) {
      fieldList = "*";
    }
    query += " | table " + fieldList;

    // Add limit
    if (this.limit > 0) {
      query += " | head " + this.limit;
    }

    return query;
  }
}
