package com.google.cloud.dataflow.pubsubtocbt;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import org.slf4j.LoggerFactory;

/**
 * Validator class for DataLoad pipeline options with specific validation for Bigtable
 * configurations.
 */
public class DataLoadOptionsValidator {

  private static final org.slf4j.Logger LOG =
      LoggerFactory.getLogger(DataLoadOptionsValidator.class);

  public static void validateOptions(DataLoadPipeline.StreamingOptions options) {
    validateBigtableOptions(options);
    validateBigtableResources(options);
  }

  private static void validateBigtableOptions(DataLoadPipeline.StreamingOptions options) {
    if (options.getBigtableInstanceId() != null) {
      if (options.getBigtableProjectId() == null || options.getBigtableProjectId().trim()
          .isEmpty()) {
        throw new IllegalArgumentException(
            "Bigtable Instance ID was provided but Bigtable project ID is missing.");
      }
    }
  }

  private static void validateBigtableResources(DataLoadPipeline.StreamingOptions options) {
    if (options.getBigtableInstanceId() != null) {
      try {
        String projectId = options.getBigtableProjectId();
        String instanceId = options.getBigtableInstanceId();
        BigtableTableAdminSettings adminSettings =
            BigtableTableAdminSettings.newBuilder()
                .setProjectId(projectId)
                .setInstanceId(instanceId)
                .build();

        try (BigtableTableAdminClient adminClient = BigtableTableAdminClient.create(
            adminSettings)) {
          String tableId = options.getBigtableTableId();

          if (tableId == null || tableId.trim().isEmpty()) {
            tableId = generateTableId(adminClient, "migrated_table");
            CreateTableRequest createTableRequest = CreateTableRequest.of(tableId).addFamily("cf1");
            adminClient.createTable(createTableRequest);
            options.setBigtableTableId(tableId);
            LOG.info("Created new table with ID: {}", tableId);
          } else {
            try {
              adminClient.getTable(tableId);
              LOG.info("Validated that table '{}' exists.", tableId);
            } catch (com.google.api.gax.rpc.NotFoundException e) {
              throw new IllegalArgumentException(
                  String.format("Table '%s' does not exist in instance '%s'", tableId, instanceId));
            }
          }
        }
      } catch (Exception e) {
        throw new IllegalArgumentException(
            "Failed to validate Bigtable resources: " + e.getMessage(), e);
      }
    }
  }

  private static String generateTableId(BigtableTableAdminClient adminClient, String prefix) {
    String baseTableId =
        prefix
            + DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
            .withZone(ZoneId.systemDefault())
            .format(Instant.now());

    String tableId = baseTableId;
    int suffix = 1;

    while (adminClient.exists(tableId)) {
      tableId = baseTableId + "_" + suffix;
      suffix++;
    }

    return tableId;
  }
}
