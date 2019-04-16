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
package org.apache.drill.exec.udfs;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.udfs.kaitai_structs.DnsPacket;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;


import javax.inject.Inject;

@FunctionTemplate(name = "parse_dns_packet",
        scope = FunctionTemplate.FunctionScope.SIMPLE,
        nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
public class DNSParserFunction implements DrillSimpleFunc {
  @Param
  VarCharHolder dns_packet;

  @Output
  BaseWriter.ComplexWriter outWriter;
  @Inject
  DrillBuf buffer;

  public void setup() {
  }

  public void eval() {
    String dns_packet_string = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(dns_packet.start, dns_packet.end, dns_packet.buffer);
    byte[] dns_bytes = dns_packet_string.getBytes();
    org.apache.drill.exec.udfs.kaitai_structs.DnsPacket data = new DnsPacket(new io.kaitai.struct.ByteBufferKaitaiStream(dns_bytes));
    org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter queryMapWriter = outWriter.rootAsMap();
    System.out.println(data.toString());
  }

  private void writeStringField(String fieldName, String value, BaseWriter.MapWriter map) {
    byte[] rowStringBytes = value.getBytes();
    buffer.reallocIfNeeded(rowStringBytes.length);
    buffer.setBytes(0, rowStringBytes);
    org.apache.drill.exec.expr.holders.VarCharHolder fieldValueHolder = new org.apache.drill.exec.expr.holders.VarCharHolder();

    fieldValueHolder.start = 0;
    fieldValueHolder.end = rowStringBytes.length;
    fieldValueHolder.buffer = buffer;

    map.varChar(fieldName).write(fieldValueHolder);
  }
}

