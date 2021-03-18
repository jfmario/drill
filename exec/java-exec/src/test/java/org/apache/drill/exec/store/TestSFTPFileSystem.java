package org.apache.drill.exec.store;

import com.github.stefanbirkner.fakesftpserver.rule.FakeSftpServerRule;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.WorkspaceConfig;
import org.apache.drill.exec.store.easy.json.JSONFormatPlugin.JSONFormatConfig;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class TestSFTPFileSystem extends ClusterTest {

  public static final String TEST_JSON = "{\"key\":\"2009-03-03\"}\n" + "{\"key\":\"2001-08-27\"}\n" + "{\"key\":\"2011-07-26\"}";

  @Rule
  public final FakeSftpServerRule sftpServer = new FakeSftpServerRule().setPort(1234);

  @BeforeClass
  public static void setup() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));
    WorkspaceConfig workspaceConfig = new WorkspaceConfig("/", false, "json", false);
    Map<String, WorkspaceConfig> workspaces = new HashMap<>();
    workspaces.put("root", workspaceConfig);

    JSONFormatConfig jsonFormatConfig = new JSONFormatConfig(null);
    Map<String, FormatPluginConfig> formatConfigs = new HashMap<>();
    formatConfigs.put("json", jsonFormatConfig);

    FileSystemConfig config = new FileSystemConfig("sftp://localhost:1234", null, workspaces, formatConfigs, null);

    cluster.defineStoragePlugin("sftp", config);
  }

  @Test
  public void testStarFile() throws Exception {
    // Load file into mock SFTP Server
    sftpServer.putFile("/test.json", TEST_JSON, StandardCharsets.UTF_8);

    String sql = "SELECT * FROM sftp.root.`test_json`";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    results.print();
  }

}
