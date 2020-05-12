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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestSplunkQueryBuilder {

  @Test
  public void testSimpleQuery() throws Exception {
    SplunkQueryBuilder builder = new SplunkQueryBuilder("main");
    String query = builder.build();
    assertEquals("search index=main | table *", query);
  }

  @Test
  public void testAddSingleFieldQuery() throws Exception {
    SplunkQueryBuilder builder = new SplunkQueryBuilder("main");
    builder.addField("field1");
    String query = builder.build();
    assertEquals("search index=main | fields field1 | table *", query);
  }

  @Test
  public void testAddMultipleFieldQuery() throws Exception {
    SplunkQueryBuilder builder = new SplunkQueryBuilder("main");
    builder.addField("field1");
    builder.addField("field2");
    builder.addField("field3");
    String query = builder.build();
    assertEquals("search index=main | fields field1,field2,field3 | table *", query);
  }

  @Test
  public void testLimitQuery() throws Exception {
    SplunkQueryBuilder builder = new SplunkQueryBuilder("main");
    builder.addLimit(5);
    String query = builder.build();
    assertEquals("search index=main | table * | head 5", query);
  }

  @Test
  public void testAddSingleSourcetypeQuery() throws Exception {
    SplunkQueryBuilder builder = new SplunkQueryBuilder("main");
    builder.addSourceType("access_combined_wcookie");
    String query = builder.build();
    assertEquals("search index=main sourcetype=\"access_combined_wcookie\" | table *", query);
  }
  @Test
  public void testAddMultipleSourcetypeQuery() throws Exception {
    SplunkQueryBuilder builder = new SplunkQueryBuilder("main");
    builder.addSourceType("access_combined_wcookie");
    builder.addSourceType("sourcetype2");

    String query = builder.build();
    assertEquals("search index=main sourcetype=\"access_combined_wcookie\" OR sourcetype=\"sourcetype2\" | table *", query);
  }
}
