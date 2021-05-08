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
package org.apache.drill.exec.store.log;

public class LogFormatPresets {
  public static LogFormatDefinition getPresetDefinition(String name) {
    switch(name) {
      case "mysql_log":
        return new LogFormatDefinition("(?<date>\\d{6})\\s(?<time>\\d{2}:\\d{2}:\\d{2})\\s+(?<processid>\\d+)\\s(?<action>\\w+)\\s+(?<query>.+)",
          new LogFormatField("date", "DATE", "yyMMdd"),
          new LogFormatField("time", "TIME", "HH:mm:ss"),
          new LogFormatField("process_id", "INT"),
          new LogFormatField("action"),
          new LogFormatField("query")
        );
      case "netscreen_log":
        return new LogFormatDefinition("(?<date>\\w{3}\\s{1,2}\\d{1,2})\\s+(?<time>\\d{2}:\\d{2}:\\d{2})\\s(?<device>[\\w\\.\\[\\]]+)\\s[\\w\\.]+:\\sNetScreen\\sdevice_id=(?<deviceid>\\w+)\\s(.+)",
          new LogFormatField("date"),
          new LogFormatField("time", "TIME", "HH:mm:ss"),
          new LogFormatField("device"),
          new LogFormatField("device_id"),
          new LogFormatField("message")
        );
      default:
        return new LogFormatDefinition();
    }
  }
}