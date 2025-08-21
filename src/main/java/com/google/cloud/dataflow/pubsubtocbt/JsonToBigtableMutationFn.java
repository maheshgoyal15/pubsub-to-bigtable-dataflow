package com.google.cloud.dataflow.pubsubtocbt;

import com.google.common.collect.ImmutableList;
import com.google.bigtable.v2.Mutation;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.google.protobuf.ByteString;
import java.io.Serializable;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.LoggerFactory;
import java.util.Base64;

public class JsonToBigtableMutationFn extends DoFn<String, KV<ByteString, Iterable<Mutation>>> implements Serializable {
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(JsonToBigtableMutationFn.class);

  @ProcessElement
  public void processElement(@Element String jsonString, OutputReceiver<KV<ByteString, Iterable<Mutation>>> out) {
    try {
      JsonObject changeEvent = JsonParser.parseString(jsonString).getAsJsonObject();

      // Assuming rowKey is always Base64-encoded now.
      String rowKeyBase64 = changeEvent.get("rowKey").getAsString();
      ByteString key = ByteString.copyFrom(Base64.getDecoder().decode(rowKeyBase64));

      String modType = changeEvent.get("modType").getAsString();

      ImmutableList.Builder<Mutation> mutations = ImmutableList.builder();

      switch (modType) {
        case "SET_CELL":
          String family = changeEvent.get("columnFamily").getAsString();
          JsonElement columnElement = changeEvent.get("column");
          JsonElement valueElement = changeEvent.get("value");
          long timestamp = changeEvent.get("timestamp").getAsLong();

          // Assuming qualifier is always Base64-encoded.
          String qualifierBase64 = columnElement.getAsString();
          // Assuming value is always Base64-encoded.
          String valueBase64 = valueElement.getAsString();

          ByteString qualifier = ByteString.copyFrom(Base64.getDecoder().decode(qualifierBase64));
          ByteString value = ByteString.copyFrom(Base64.getDecoder().decode(valueBase64));

          LOG.info("Processing SET_CELL for row: {}, family: {}, qualifier: {}", key, family, qualifier);

          Mutation.SetCell setCell = Mutation.SetCell.newBuilder()
              .setFamilyName(family)
              .setColumnQualifier(qualifier)
              .setTimestampMicros(timestamp)
              .setValue(value)
              .build();

          mutations.add(Mutation.newBuilder().setSetCell(setCell).build());
          break;

        case "DELETE_CELLS":
          String deleteCellsFamily = changeEvent.get("columnFamily").getAsString();
          String deleteCellsQualifierBase64 = changeEvent.get("column").getAsString();

          ByteString deleteCellsQualifier = ByteString.copyFrom(Base64.getDecoder().decode(deleteCellsQualifierBase64));

          LOG.info("Processing DELETE_CELLS for row: {}, family: {}, qualifier: {}", key, deleteCellsFamily, deleteCellsQualifier);

          Mutation.DeleteFromColumn deleteCells = Mutation.DeleteFromColumn.newBuilder()
              .setFamilyName(deleteCellsFamily)
              .setColumnQualifier(deleteCellsQualifier)
              .build();

          mutations.add(Mutation.newBuilder().setDeleteFromColumn(deleteCells).build());
          break;

        case "DELETE_FAMILY":
          String deleteFamilyName = changeEvent.get("columnFamily").getAsString();
          LOG.info("Processing DELETE_FAMILY for row: {}, family: {}", key, deleteFamilyName);

          Mutation.DeleteFromFamily deleteFamily = Mutation.DeleteFromFamily.newBuilder()
              .setFamilyName(deleteFamilyName)
              .build();

          mutations.add(Mutation.newBuilder().setDeleteFromFamily(deleteFamily).build());
          break;

        case "DELETE_ROW":
          LOG.info("Processing DELETE_ROW for row: {}", key);
          mutations.add(Mutation.newBuilder()
              .setDeleteFromRow(Mutation.DeleteFromRow.newBuilder().build())
              .build());
          break;

        default:
          LOG.warn("Unsupported change stream modType: {}", modType);
          break;
      }
      out.output(KV.of(key, mutations.build()));

    } catch (JsonSyntaxException | IllegalArgumentException | NullPointerException e) {
      LOG.error("Failed to parse JSON change stream event. Ensure all expected fields are present and correctly Base64-encoded: {}", jsonString, e);
    } catch (Exception e) {
      LOG.error("An unexpected error occurred while processing change stream event: {}", e.getMessage(), e);
    }
  }
}