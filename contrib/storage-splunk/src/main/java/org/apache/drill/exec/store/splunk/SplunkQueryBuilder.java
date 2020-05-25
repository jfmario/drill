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
  private String filters;
  private int filterCount;
  private int limit;

  public SplunkQueryBuilder (String index) {
    this.index = index;
    this.filters = "";
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
   * This method should only be called once, but if is called more than once,
   * it will set the limit to the most recent value.
   * @param limit Positive, non-zero integer of number of desired rows.
   * @return SplunkQueryBuilder with limit set.
   */
  public SplunkQueryBuilder addLimit(int limit) {
    if (limit > 0) {
      this.limit = limit;
    }
    return this;
  }

  /**
   * Adds a filter to the Splunk query.  Splunk treats all filters as
   * AND filters, without explicitly noting that.
   * @param left The field to be filtered
   * @param right The value of that field
   * @return
   */
  public SplunkQueryBuilder addEqualityFilter(String left, String right) {
    filters = filters + " " + left + "=" + right;
    filterCount++;
    return this;
  }

  private String quoteString(String word) {
    return "\"" + word + "\"";
  }


  public String build() {
    // Add the sourcetype
    if (! Strings.isNullOrEmpty(sourceTypes)) {
      query += sourceTypes;
    }

    // Add filters
    if (! Strings.isNullOrEmpty(filters)) {
      query += filters;
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
