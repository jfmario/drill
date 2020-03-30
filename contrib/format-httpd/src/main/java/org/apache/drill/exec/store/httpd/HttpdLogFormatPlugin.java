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

import java.util.List;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.hadoop.conf.Configuration;
import org.apache.drill.exec.store.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpdLogFormatPlugin extends EasyFormatPlugin<HttpdLogFormatConfig> {

  protected static final String DEFAULT_NAME = "httpd";

  private static final Logger logger = LoggerFactory.getLogger(HttpdLogFormatPlugin.class);

  public HttpdLogFormatPlugin(final String name,
                              final DrillbitContext context,
                              final Configuration fsConf,
                              final StoragePluginConfig storageConfig,
                              final HttpdLogFormatConfig formatConfig) {

    super(name, easyConfig(fsConf, formatConfig), context, storageConfig, formatConfig);
  }

  private static EasyFormatConfig easyConfig(Configuration fsConf, HttpdLogFormatConfig pluginConfig) {
    EasyFormatConfig config = new EasyFormatConfig();
    config.readable = true;
    config.writable = false;
    config.blockSplittable = false;
    config.compressible = true;
    config.supportsProjectPushdown = true;
    config.extensions = pluginConfig.getExtensions();
    config.fsConf = fsConf;
    config.defaultName = DEFAULT_NAME;
    config.readerOperatorType = UserBitShared.CoreOperatorType.HTPPD_LOG_SUB_SCAN_VALUE;
    config.useEnhancedScan = false;  // Set to true later
    return config;
  }

  @Override
  public RecordReader getRecordReader(final FragmentContext context, final DrillFileSystem dfs,
      final FileWork fileWork, final List<SchemaPath> columns, final String userName) {
    return new HttpdLogRecordReader(context, dfs, fileWork, columns, this);
  }
}
