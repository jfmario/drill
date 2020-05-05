package org.apache.drill.exec.store.splunk;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.store.AbstractSchemaFactory;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.AbstractSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class SplunkSchemaFactory extends AbstractSchemaFactory {

  private static final Logger logger = LoggerFactory.getLogger(SplunkSchemaFactory.class);

  private final SplunkFormatPlugin plugin;

  private final Map<String, DynamicDrillTable> activeTables = new HashMap<>();

  public SplunkSchemaFactory(SplunkFormatPlugin plugin) {
    super(plugin.getName());
    this.plugin = plugin;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    SplunkDefaultSchema schema = new SplunkDefaultSchema(plugin);
    logger.debug("Registering {} {}", schema.getName(), schema.toString());

    SchemaPlus schemaPlus = parent.add(getName(), schema);
    //schema.setHolder(schemaPlus);
  }

  class SplunkDefaultSchema extends AbstractSchema {

    private final SplunkFormatPlugin plugin;

    public SplunkDefaultSchema(SplunkFormatPlugin plugin) {
      super(Collections.emptyList(), plugin.getName());
      this.plugin = plugin;
    }

    @Override
    public Table getTable(String name) {
      /*DynamicDrillTable table = activeTables.get(name);
      if (table != null) {
        return table;
      }
      if (MY_TABLE.contentEquals(name)) {
        return null; // TODO
      }*/
      return null; // Unknown table
    }

    private DynamicDrillTable registerTable(String name, DynamicDrillTable table) {
      activeTables.put(name, table);
      return table;
    }

    @Override
    public String getTypeName() {
      return SplunkFormatConfig.NAME;
    }
  }
}
