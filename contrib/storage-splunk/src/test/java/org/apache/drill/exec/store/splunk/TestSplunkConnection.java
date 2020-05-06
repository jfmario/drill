package org.apache.drill.exec.store.splunk;

import com.splunk.EntityCollection;
import com.splunk.Index;
import org.apache.drill.common.exceptions.UserException;
import org.junit.Test;

import static org.junit.Assert.fail;

public class TestSplunkConnection {

  @Test
  public void testConnection() throws Exception {
    SplunkConnection sc = new SplunkConnection(new SplunkPluginConfig("admin", "password", "localhost", 8089, null, null));
    sc.connect();
  }

  @Test
  public void testConnectionFail() throws Exception {
    try {
      SplunkConnection sc = new SplunkConnection(new SplunkPluginConfig("hacker", "hacker", "localhost", 8089, null, null));
      sc.connect();
      fail();
    } catch (UserException e) {

    }
  }

  @Test
  public void testGetIndexes() throws Exception {
    SplunkConnection sc = new SplunkConnection(new SplunkPluginConfig("admin", "password", "localhost", 8089, null, null));
    EntityCollection<Index> indexes = sc.getIndexes();
    for (Index index : indexes.values()) {
      System.out.println(index.getName());
    }
  }
}
