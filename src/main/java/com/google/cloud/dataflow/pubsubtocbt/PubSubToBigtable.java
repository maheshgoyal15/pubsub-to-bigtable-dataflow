// package com.google.cloud.dataflow.pubsubtocbt;
//
// import com.google.cloud.bigtable.data.v2.models.Mutation;
// import com.google.gson.Gson;
// import com.google.gson.JsonSyntaxException;
// import com.google.protobuf.ByteString;
// import org.apache.beam.sdk.Pipeline;
// import org.apache.beam.sdk.PipelineResult;
// import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
// import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
// import org.apache.beam.sdk.options.PipelineOptions;
// import org.apache.beam.sdk.options.PipelineOptionsFactory;
// import org.apache.beam.sdk.transforms.DoFn;
// import org.apache.beam.sdk.transforms.PTransform;
// import org.apache.beam.sdk.transforms.GroupByKey;
// import org.apache.beam.sdk.transforms.ParDo;
// import org.apache.beam.sdk.values.KV;
// import org.apache.beam.sdk.values.PCollection;
// import org.apache.beam.sdk.values.PCollectionTuple;
// import org.apache.beam.sdk.values.PDone;
// import org.apache.beam.sdk.values.TupleTag;
// import org.apache.beam.sdk.values.TupleTagList;
//
// import java.io.Serializable;
// import java.util.Arrays;
// import java.util.List;
// import java.util.logging.Level;
// import java.util.logging.Logger;
//
// public class PubSubToBigtable {
//
//     private static final Logger LOG = Logger.getLogger(PubSubToBigtable.class.getName());
//     private static final Gson GSON = new Gson();
//
//     // Updated TupleTag declarations to use the traditional approach
//     private static final TupleTag<KV<ByteString, Mutation>> SUCCESSFUL_MUTATIONS =
//         new TupleTag<KV<ByteString, Mutation>>() {};
//     private static final TupleTag<String> FAILED_MESSAGES = new TupleTag<String>() {};
//
//     public static PipelineResult run(PubSubToBigtableOptions options) {
//         Pipeline pipeline = Pipeline.create(options);
//
//         // Step 1: Read from Pub/Sub
//         PCollection<String> inputMessages = pipeline
//             .apply("Read from Pub/Sub", PubsubIO.readStrings().fromTopic(options.getInputTopic()));
//
//         // Step 2: Apply the custom PTransform that handles all conversion and writing logic
//         inputMessages.apply("Convert and Write to Bigtable", new WriteToBigtableTransform(options));
//
//         return pipeline.run();
//     }
//
//     /**
//      * A custom PTransform that encapsulates all the logic for converting JSON messages
//      * and writing them to Bigtable.
//      */
//     static class WriteToBigtableTransform extends PTransform<PCollection<String>, PDone> {
//         private final PubSubToBigtableOptions options;
//
//         WriteToBigtableTransform(PubSubToBigtableOptions options) {
//             this.options = options;
//         }
//
//         @Override
//         public PDone expand(PCollection<String> input) {
//             // Transform messages to individual mutations
//             PCollectionTuple transformOutputs = input
//                 .apply("Transform to Bigtable Mutations", ParDo.of(new JsonToMutationFn())
//                     .withOutputTags(SUCCESSFUL_MUTATIONS, TupleTagList.of(FAILED_MESSAGES)));
//
//             PCollection<KV<ByteString, Mutation>> successfulMutations = transformOutputs.get(SUCCESSFUL_MUTATIONS);
//             PCollection<String> failedMessages = transformOutputs.get(FAILED_MESSAGES);
//
//             // Group mutations by key to get the format BigtableIO expects
//             PCollection<KV<ByteString, Iterable<Mutation>>> groupedMutations = successfulMutations
//                 .apply("Group By Key", org.apache.beam.sdk.transforms.GroupByKey.<ByteString, Mutation>create());
//
//             // Apply BigtableIO.write() to the grouped mutations
//             groupedMutations.apply(BigtableIO.write()
//                 .withProjectId(options.getBigtableProjectId())
//                 .withInstanceId(options.getBigtableInstanceId())
//                 .withTableId(options.getBigtableTableId()));
//
//             // Write failed messages to dead-letter queue
//             failedMessages.apply("Write to Dead Letter Topic",
//                 PubsubIO.writeStrings().to(options.getDeadLetterTopic()));
//
//             return PDone.in(input.getPipeline());
//         }
//     }
//
//     /**
//      * DoFn that parses JSON and outputs KV<ByteString, Mutation>
//      * We'll use the traditional GroupByKey approach for compatibility.
//      */
//     static class JsonToMutationFn extends DoFn<String, KV<ByteString, Mutation>> {
//
//         @ProcessElement
//         public void processElement(ProcessContext c) {
//             String json = c.element();
//             try {
//                 Mod mod = GSON.fromJson(json, Mod.class);
//                 if (mod.getRowkey() == null || mod.getRowkey().isEmpty()) {
//                     throw new IllegalArgumentException("Rowkey cannot be null or empty.");
//                 }
//
//                 Mutation mutation = Mutation.create();
//                 switch (mod.getModType()) {
//                     case SET_CELL:
//                         mutation.setCell(
//                             mod.getColumnFamily(),
//                             mod.getColumnQualifier(),
//                             mod.getTimestamp(),
//                             mod.getValue());
//                         break;
//                     case DELETE_CELLS:
//                         mutation.deleteCells(
//                             mod.getColumnFamily(),
//                             mod.getColumnQualifier());
//                         break;
//                     case DELETE_FAMILY:
//                         mutation.deleteFamily(mod.getColumnFamily());
//                         break;
//                     default:
//                         throw new IllegalArgumentException("Unsupported ModType: " + mod.getModType());
//                 }
//
//                 // Output individual mutations - we'll group them later
//                 c.output(SUCCESSFUL_MUTATIONS, KV.of(ByteString.copyFromUtf8(mod.getRowkey()), mutation));
//
//             } catch (JsonSyntaxException | IllegalArgumentException e) {
//                 LOG.log(Level.SEVERE, "Failed to parse JSON message: " + json + ". Sending to dead-letter queue.", e);
//                 c.output(FAILED_MESSAGES, json);
//             }
//         }
//     }
//
//     /**
//      * A simple POJO to represent a Bigtable change for serialization.
//      */
//     public static class Mod implements Serializable {
//         private String rowkey;
//         private String columnFamily;
//         private String columnQualifier;
//         private long timestamp;
//         private String value;
//         private ModType modType;
//
//         // Default constructor for JSON deserialization
//         public Mod() {}
//
//         // Getters and Setters
//         public String getRowkey() { return rowkey; }
//         public void setRowkey(String rowkey) { this.rowkey = rowkey; }
//         public String getColumnFamily() { return columnFamily; }
//         public void setColumnFamily(String columnFamily) { this.columnFamily = columnFamily; }
//         public String getColumnQualifier() { return columnQualifier; }
//         public void setColumnQualifier(String columnQualifier) { this.columnQualifier = columnQualifier; }
//         public long getTimestamp() { return timestamp; }
//         public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
//         public String getValue() { return value; }
//         public void setValue(String value) { this.value = value; }
//         public ModType getModType() { return modType; }
//         public void setModType(ModType modType) { this.modType = modType; }
//
//         @Override
//         public String toString() {
//             return "Mod{" +
//                 "rowkey='" + rowkey + '\'' +
//                 ", columnFamily='" + columnFamily + '\'' +
//                 ", columnQualifier='" + columnQualifier + '\'' +
//                 ", timestamp=" + timestamp +
//                 ", value='" + value + '\'' +
//                 ", modType=" + modType +
//                 '}';
//         }
//     }
//
//     /**
//      * Enum to represent the type of mutation.
//      */
//     public enum ModType {
//         SET_CELL,
//         DELETE_CELLS,
//         DELETE_FAMILY,
//         UNKNOWN
//     }
//
//     /**
//      * Pipeline options interface for PubSub to Bigtable.
//      * You'll need to implement this interface or use your existing options class.
//      */
//     public interface PubSubToBigtableOptions extends PipelineOptions {
//         String getInputTopic();
//         void setInputTopic(String value);
//
//         String getDeadLetterTopic();
//         void setDeadLetterTopic(String value);
//
//         String getBigtableProjectId();
//         void setBigtableProjectId(String value);
//
//         String getBigtableInstanceId();
//         void setBigtableInstanceId(String value);
//
//         String getBigtableTableId();
//         void setBigtableTableId(String value);
//     }
//
//     /**
//      * Main method for running the pipeline.
//      */
//     public static void main(String[] args) {
//         PubSubToBigtableOptions options = PipelineOptionsFactory.fromArgs(args)
//             .withValidation()
//             .as(PubSubToBigtableOptions.class);
//
//         run(options);
//     }
// }