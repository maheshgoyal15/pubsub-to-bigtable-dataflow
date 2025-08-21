#!/bin/bash

# This script runs a Dataflow job using Maven with autoscaling enabled.

# Main class for the Dataflow pipeline
MAIN_CLASS="com.google.cloud.dataflow.pubsubtocbt.DataLoadPipeline"

# Dataflow pipeline arguments
RUNNER="DataflowRunner"
PROJECT="your-gcp-project-id"
REGION="your-region"
TEMP_LOCATION="gs://your-gcs-bucket/temp"
INPUT_SUBSCRIPTION="projects/your-gcp-project-id/subscriptions/your-pubsub-subscription"
BIGTABLE_PROJECT_ID="your-gcp-project-id"
BIGTABLE_INSTANCE_ID="your-bigtable-instance"
BIGTABLE_TABLE_ID="your-bigtable-table"

# Autoscaling arguments
AUTOSCALING_ALGORITHM="THROUGHPUT_BASED"
NUM_WORKERS=15
MAX_NUM_WORKERS=30

# Construct the full exec.args string
EXEC_ARGS="--runner=$RUNNER --project=$PROJECT --region=$REGION --tempLocation=$TEMP_LOCATION --inputSubscription=$INPUT_SUBSCRIPTION --bigtableProjectId=$BIGTABLE_PROJECT_ID --bigtableInstanceId=$BIGTABLE_INSTANCE_ID --bigtableTableId=$BIGTABLE_TABLE_ID --autoscalingAlgorithm=$AUTOSCALING_ALGORITHM --numWorkers=$NUM_WORKERS --maxNumWorkers=$MAX_NUM_WORKERS"

# Arguments for the Java Virtual Machine (VM)
VM_ARGS="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/jdk.internal.reflect=ALL-UNNAMED"

# Run the Maven command
mvn compile exec:java \
  -Dexec.mainClass="$MAIN_CLASS" \
  -Dexec.args="$EXEC_ARGS" \
  -Dexec.vmargs="$VM_ARGS" \
  -Dexec.cleanupDaemonThreads=false