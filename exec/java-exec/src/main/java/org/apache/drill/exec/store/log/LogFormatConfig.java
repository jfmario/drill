package org.apache.drill.exec.store.log;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Objects;
import org.apache.drill.common.logical.FormatPluginConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@JsonTypeName("log")
public class LogFormatConfig implements FormatPluginConfig {

  public String regex;
  public List<String> fields = new ArrayList<>();
  public String extension;
  public List<String> dataTypes = new ArrayList<>();
  public String dateFormat;
  public String timeFormat;
  public int maxErrors = 10;

  public String getRegex() {
    return regex;
  }

  public List<String> getFields() {
    return fields;
  }

  public String getExtension() {
    return extension;
  }

  public List<String> getDataTypes() {
    return dataTypes;
  }

  public String getDateFormat() {
    return dateFormat;
  }

  public String getTimeFormat() {
    return timeFormat;
  }

  public int getMaxErrors() {
    return maxErrors;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    LogFormatConfig other = (LogFormatConfig) obj;
    return Objects.equal(regex, other.regex) &&
        Objects.equal(fields, other.fields) &&
        Objects.equal(dataTypes, other.dataTypes) &&
        Objects.equal(dateFormat, other.dateFormat) &&
        Objects.equal(timeFormat, other.timeFormat) &&
        Objects.equal(maxErrors, other.maxErrors) &&
        Objects.equal(extension, other.extension);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(new Object[]{regex, fields, extension});
  }
}