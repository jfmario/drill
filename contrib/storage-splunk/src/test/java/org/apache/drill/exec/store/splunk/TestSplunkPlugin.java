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

import org.apache.drill.exec.physical.rowSet.DirectRowSet;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryRowSetIterator;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSplunkPlugin extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));

    StoragePluginRegistry pluginRegistry = cluster.drillbit().getContext().getStorage();
    SplunkPluginConfig config = new SplunkPluginConfig( "cgivre", "password", "localhost", 8089, null, null);
    config.setEnabled(true);
    pluginRegistry.put(SplunkPluginConfig.NAME, config);
  }

  @Test
  public void testStarQuery() throws Exception {
    String sql = "SELECT * FROM splunk.main LIMIT 5";
    QueryRowSetIterator results = client.queryBuilder().sql(sql).rowSetIterator();
    while (results.hasNext()) {
      DirectRowSet result = results.next();
      result.print();
    }
  }

  @Test
  public void testExplictFieldsQuery() throws Exception {
    String sql = "SELECT _time, clientip, file, host FROM splunk.main";
    QueryRowSetIterator results = client.queryBuilder().sql(sql).rowSetIterator();
    while (results.hasNext()) {
      DirectRowSet result = results.next();
      result.print();
    }
  }

  @Test
  public void testExplictFieldsWithLimitQuery() throws Exception {
    String sql = "SELECT _time, clientip, file, host FROM splunk.main LIMIT 10";
    QueryRowSetIterator results = client.queryBuilder().sql(sql).rowSetIterator();
    while (results.hasNext()) {
      DirectRowSet result = results.next();
      result.print();
    }
  }

  @Test
  public void testExplictFieldsWithOneFieldLimitQuery() throws Exception {
    String sql = "SELECT clientip FROM splunk.main LIMIT 2";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    results.print();
    /*while (results.hasNext()) {
      DirectRowSet result = results.next();
      result.print();
    }*/
  }

  @Test
  public void testEqualityFilterQuery() throws Exception {
    String sql = "SELECT _time, clientip, file, host FROM splunk.main WHERE file='cart.do'";
    QueryRowSetIterator results = client.queryBuilder().sql(sql).rowSetIterator();
    while (results.hasNext()) {
      DirectRowSet result = results.next();
      result.print();
    }
  }
}
