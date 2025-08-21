package com.google.cloud.dataflow.pubsubtocbt;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Defines common options for pipeline execution.
 */
public interface BaseOptions extends PipelineOptions {

  @Description("BigTable Project Id.")
  String getBigtableProjectId();

  void setBigtableProjectId(String value);

  @Description("BigTable Instance Id.")
  String getBigtableInstanceId();

  void setBigtableInstanceId(String value);

  @Description("BigTable Table Id.")
  String getBigtableTableId();

  void setBigtableTableId(String value);
}
