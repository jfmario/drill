package org.apache.drill.exec.store.splunk;

import org.apache.drill.shaded.guava.com.google.common.collect.Sets;
import com.splunk.EntityCollection;
import com.splunk.Index;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.AbstractSchemaFactory;
import org.apache.drill.exec.store.SchemaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SplunkSchemaFactory extends AbstractSchemaFactory {

  private static final Logger logger = LoggerFactory.getLogger(SplunkSchemaFactory.class);
  private final SplunkStoragePlugin plugin;
  private final SplunkConnection connection;
  private EntityCollection<Index> indexes;

  public SplunkSchemaFactory(SplunkStoragePlugin plugin) {
    super(plugin.getName());
    this.plugin = plugin;
    SplunkPluginConfig config = plugin.getConfig();
    this.connection = new SplunkConnection(config);

    // Get Splunk Indexes
    connection.connect();
    indexes = connection.getIndexes();
  }

  @Override

  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) {
    SplunkSchema schema = new SplunkSchema(plugin);
    SchemaPlus plusOfThis = parent.add(schema.getName(), schema);
    // schema.setPlus(plusOfThis);

    /*for (Index index : indexes.values()) {
      SplunkSchema schema = new SplunkSchema(plugin);

      SchemaPlus schemaPlus = parent.add(getName(), schema);
      logger.debug("Registering {}", index.getName());
      //schemaPlus.add(index.getName(), schema);
    }*/
  }

  class SplunkSchema extends AbstractSchema {

    private final Map<String, DynamicDrillTable> activeTables = new HashMap<>();
    private final SplunkStoragePlugin plugin;
    private final Set<String> indexNames;

    public SplunkSchema(SplunkStoragePlugin plugin) {
      super(Collections.emptyList(), plugin.getName());
      this.plugin = plugin;
      this.indexNames = getIndexNames();
      registerIndexes();

    }

    @Override
    public Table getTable(String name) {
      DynamicDrillTable table = activeTables.get(name);
      if (table != null) {
        // If the table was found, return it.
        return table;
      } else {
        // Register the table
        return registerTable(name, new DynamicDrillTable(plugin, plugin.getName(),
          new SplunkScanSpec(plugin.getName(), name, plugin.getConfig())));
      }
    }

    @Override
    public boolean showInInformationSchema() {
      return true;
    }

    @Override
    public Set<String> getTableNames() {
      return Sets.newHashSet(activeTables.keySet());
    }

    private DynamicDrillTable registerTable(String name, DynamicDrillTable table) {
      activeTables.put(name, table);
      return table;
    }

    @Override
    public String getTypeName() {
      return SplunkPluginConfig.NAME;
    }

    private Set<String> getIndexNames() {
      List<String> indexNames = new ArrayList<>();
      for (String index : indexes.keySet()) {
        indexNames.add(index);
      }
      return new HashSet<>(indexNames);
    }

    private void registerIndexes() {
      for (String indexName : indexes.keySet()) {
        logger.debug("Registering {}", indexName);
        registerTable(indexName, new DynamicDrillTable(plugin, plugin.getName(),
          new SplunkScanSpec(plugin.getName(), indexName, plugin.getConfig())));
      }
    }
  }
}
