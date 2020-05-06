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

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;

import java.io.IOException;

public class SplunkStoragePlugin extends AbstractStoragePlugin {

  private final SplunkPluginConfig config;
  private final SplunkSchemaFactory schemaFactory;

  public SplunkStoragePlugin(SplunkPluginConfig configuration, DrillbitContext context, String name) {
    super(context, name);
    this.config = configuration;
    this.schemaFactory = new SplunkSchemaFactory(this);
  }

  @Override
  public SplunkPluginConfig getConfig() {
    return config;
  }

  @Override
  public boolean supportsRead() {
    return true;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    schemaFactory.registerSchemas(schemaConfig, parent);
  }

  @Override
  public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection) throws IOException {
    SplunkScanSpec scanSpec = selection.getListWith(context.getLpPersistence().getMapper(), new TypeReference<SplunkScanSpec>() {});
    return new SplunkGroupScan(scanSpec);
  }
}
