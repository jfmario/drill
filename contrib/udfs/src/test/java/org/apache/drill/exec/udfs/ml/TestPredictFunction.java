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

package org.apache.drill.exec.udfs.ml;


import org.apache.drill.categories.SqlFunctionTest;
import org.apache.drill.categories.UnlikelyTest;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;


import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({UnlikelyTest.class, SqlFunctionTest.class})
public class TestPredictFunction extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher);
    startCluster(builder);
  }

  @Test
  public void testPrediction() throws Exception {

    Path resourceDirectory = Paths.get("src","test","resources", "sample-data");
    String absolutePath = resourceDirectory.toFile().getAbsolutePath();

    String query = "SELECT predict( '" + absolutePath + "/GBMModel.zip', 48.0, 1.0, 21.0, 1.0, 7590.0, 0.0, 1.0, 2.0, " +
      "40" +
      ".0, 1.0, 0.0, 0.0 ) " +
      "AS " +
      "score FROM (VALUES(1))";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("score")
      .baselineValues(14000.0)
      .go();
  }

  @Test
  public void testReadException() throws Exception {
    String query = "SELECT nearestDate( TO_TIMESTAMP('2019-02-01 07:22:00', 'yyyy-MM-dd HH:mm:ss'), 'BAD_DATE') AS nearest_year " +
      "FROM (VALUES(1))";
    try {
      run(query);
      fail();
    } catch (DrillRuntimeException e) {
      //assertTrue(e.getMessage().contains("[BAD_DATE] is not a valid time statement. Expecting: " + Arrays.asList(NearestDateUtils.TimeInterval.values())));
    }
  }
}
