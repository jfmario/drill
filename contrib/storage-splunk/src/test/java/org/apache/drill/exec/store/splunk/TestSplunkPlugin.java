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

import org.apache.drill.categories.SlowTest;
import org.apache.drill.categories.SplunkStorageTest;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

//@Ignore("These tests require a running Splunk instance.")
@Category({SlowTest.class, SplunkStorageTest.class})
public class TestSplunkPlugin extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));

    StoragePluginRegistry pluginRegistry = cluster.drillbit().getContext().getStorage();
    SplunkPluginConfig config = new SplunkPluginConfig( "cgivre", "password", "localhost", 8089, "-60d", null);
    config.setEnabled(true);
    pluginRegistry.put(SplunkPluginConfig.NAME, config);
  }

  @Test
  public void verifyPluginConfig() throws Exception {
    String sql = "SELECT SCHEMA_NAME, TYPE FROM INFORMATION_SCHEMA.`SCHEMATA` WHERE TYPE='splunk'\n" +
      "ORDER BY SCHEMA_NAME";

    RowSet results = client.queryBuilder().sql(sql).rowSet();
    results.print();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("SCHEMA_NAME", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .add("TYPE", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow("splunk", "splunk")
      .build();

    RowSetUtilities.verify(expected, results);
  }

  @Test
  public void verifyIndexes() throws Exception {
    String sql = "SHOW TABLES IN `splunk`";

    RowSet results = client.queryBuilder().sql(sql).rowSet();
    results.print();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("TABLE_SCHEMA", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .add("TABLE_NAME", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow("splunk", "summary")
      .addRow("splunk", "splunklogger")
      .addRow("splunk", "_thefishbucket")
      .addRow("splunk", "_audit")
      .addRow("splunk", "_internal")
      .addRow("splunk", "_introspection")
      .addRow("splunk", "main")
      .addRow("splunk", "history")
      .addRow("splunk", "_telemetry")
      .build();

    RowSetUtilities.verify(expected, results);
  }

  @Test
  public void testStarQuery() throws Exception {
    String sql = "SELECT * FROM splunk.main LIMIT 5";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    results.print();
  }

  @Test
  public void testExplictFieldsQuery() throws Exception {
    String sql = "SELECT _time, clientip, file, host FROM splunk.main";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
  }

  @Test
  public void testExplictFieldsWithLimitQuery() throws Exception {
    String sql = "SELECT _time, clientip, file, host FROM splunk.main LIMIT 10";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
  }

  @Test
  public void testExplictFieldsWithSourcetype() throws Exception {
    String sql = "SELECT _time, clientip, file, host FROM splunk.main WHERE sourcetype='access_combined_wcookie' LIMIT 10";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
  }

  @Test
  public void testExplictFieldsWithOneFieldLimitQuery() throws Exception {
    String sql = "SELECT clientip FROM splunk.main LIMIT 2";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
  }

  @Test
  public void testSingleEqualityFilterQuery() throws Exception {
    String sql = "SELECT _time, clientip, file, host FROM splunk.main WHERE file='cart.do'";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
  }

  @Test
  public void testMultipleEqualityFilterQuery() throws Exception {
    String sql = "SELECT _time, clientip, file, host FROM splunk.main WHERE file='cart.do' AND clientip='217.15.20.146'";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
  }

  @Test
  public void testFilterOnUnProjectedColumnQuery() throws Exception {
    String sql = "SELECT _time, clientip, host FROM splunk.main WHERE file='cart.do'";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
  }

  @Test
  public void testSerDe() throws Exception {
    String sql = "SELECT COUNT(*) FROM splunk.main WHERE sourcetype='access_combined_wcookie' AND file='cart.do' AND clientip='217.15.20.146'";
    String plan = queryBuilder().sql(sql).explainJson();
    long cnt = queryBuilder().physical(plan).singletonLong();
    assertEquals("Counts should match", 164L, cnt);
  }
}
