package org.apache.hadoop.hive.druid;

import org.apache.hadoop.hive.druid.io.DruidLuceneOutputFormat;
import org.apache.hadoop.mapred.OutputFormat;

public class DruidLuceneStorageHandler extends DruidStorageHandler {
  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return DruidLuceneOutputFormat.class;
  }
}
