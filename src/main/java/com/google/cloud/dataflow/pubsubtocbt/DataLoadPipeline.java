package com.google.cloud.dataflow.pubsubtocbt;

import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Mutation.DeleteFromRow;
import com.google.bigtable.v2.Mutation.SetCell;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.slf4j.LoggerFactory;
import com.google.common.collect.ImmutableList;

/**
 * This class defines the DataLoad pipeline options and the main entry point for the data loading
 * process.
 */
public class DataLoadPipeline {
  public interface StreamingOptions extends BaseOptions {
    @Description("The Pub/Sub subscription to read from.")
    String getInputSubscription();

    void setInputSubscription(String value);

    @Description("Maximum mutations per row. This is now handled automatically by BigtableIO, but can be set to override the default.")
    @Default.Integer(100000)
    int getBigtableMaxMutationsPerRow();

    void setBigtableMaxMutationsPerRow(int value);
  }

  /**
   * The main entry point for the data validation pipeline.
   *
   * @param args command-line arguments for the pipeline.
   */
  public static void main(String[] args) {
    StreamingOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(StreamingOptions.class);

    DataLoadOptionsValidator.validateOptions(options);

    Pipeline pipeline = Pipeline.create(options);

    // Read from Pub/Sub
    PCollection<String> pubsubMessages =
        pipeline.apply(
            "ReadFromPubSub", PubsubIO.readStrings().fromSubscription(options.getInputSubscription()));

    // Convert messages to Bigtable Mutations
    PCollection<KV<ByteString, Iterable<Mutation>>> bigtableMutations =
        pubsubMessages.apply(
            "ConvertJsonToBigtableMutations", ParDo.of(new JsonToBigtableMutationFn()));

    // Write to Bigtable
    bigtableMutations.apply(
        "WriteToBigtable", new WriteToBigtable());

    pipeline.run();
  }
}

