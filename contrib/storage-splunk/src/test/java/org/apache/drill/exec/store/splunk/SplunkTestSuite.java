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
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;


@RunWith(Suite.class)
@Suite.SuiteClasses({
  TestSplunkPlugin.class,
  TestSplunkQueryBuilder.class,
  TestSplunkConnection.class
})

@Category({SlowTest.class, SplunkStorageTest.class})
public class SplunkTestSuite {
  private static SplunkPluginConfig splunkStoragePluginConfig = null;

  @BeforeClass
  public static void initSplunk() {
    splunkStoragePluginConfig= new SplunkPluginConfig( "admin", "password", "localhost", 8089, "-30d", null);
    splunkStoragePluginConfig.setEnabled(true);
  }

  public static SplunkPluginConfig getSplunkStoragePluginConfig() {
    return splunkStoragePluginConfig;
  }
}