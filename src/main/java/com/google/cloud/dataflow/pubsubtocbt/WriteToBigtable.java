package com.google.cloud.dataflow.pubsubtocbt;

import com.google.bigtable.v2.Mutation;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.slf4j.LoggerFactory;

/**
 * A PTransform that writes Bigtable Mutations to Google Cloud Bigtable.
 */
public class WriteToBigtable extends
    PTransform<PCollection<KV<ByteString, Iterable<Mutation>>>, PDone> {

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(WriteToBigtable.class);

  @Override
  public PDone expand(PCollection<KV<ByteString, Iterable<Mutation>>> bigtableMutations) {
    DataLoadPipeline.StreamingOptions options =
        bigtableMutations.getPipeline().getOptions().as(DataLoadPipeline.StreamingOptions.class);

    BigtableIO.Write bigtableWriter =
        BigtableIO.write()
            .withProjectId(options.getBigtableProjectId())
            .withInstanceId(options.getBigtableInstanceId())
            .withTableId(options.getBigtableTableId());

    return bigtableMutations.apply("Write To Bigtable", bigtableWriter);
  }
}
