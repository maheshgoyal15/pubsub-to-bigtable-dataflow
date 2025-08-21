# Bigtable Change Stream Replication Pipeline

This repository contains a streaming Dataflow pipeline that replicates data from a Google Cloud Bigtable source to a target Bigtable table. The solution is designed for low-latency data synchronization and handles various data types, including binary and special characters, without corruption.

The pipeline is the second part of a two-stage data replication process:

1. **Source:** A separate Dataflow job (using the [Bigtable Change Streams to Pub/Sub template](https://cloud.google.com/dataflow/docs/guides/templates/provided/cloud-bigtable-change-streams-to-pubsub)) captures changes from a source Bigtable table and publishes them as Base64-encoded JSON messages to a Pub/Sub topic.

2. **Target (This Project):** This pipeline consumes the messages from Pub/Sub, decodes the Base64 data, and applies the corresponding mutations to a destination Bigtable table.

---

## Deployment

This project uses Maven to build and run the Dataflow job. A deployment script is provided to simplify the process.

### Prerequisites

* **Java 8 or higher** installed.

* **Maven** installed.

* **Google Cloud CLI (`gcloud`)** installed and authenticated.

* An active Google Cloud Project with the **Dataflow API**, **Pub/Sub API**, and **Cloud Bigtable API** enabled.

* A **Pub/Sub subscription** configured to receive messages from your Bigtable change stream.

* A **target Bigtable table** with the necessary column families.

### Deployment Steps

1.  **Clone the Repository**

    ```bash
    git clone https://github.com/maheshgoyal15/pubsub-to-bigtable-dataflow.git
    cd pubsub-to-bigtable-dataflow
    ```

2.  **Configure the Deployment Script**
    Open the provided `run_dataflow.sh` script and update the following variables with your specific project details:

    * `PROJECT`
    * `REGION`
    * `TEMP_LOCATION`
    * `INPUT_SUBSCRIPTION`
    * `BIGTABLE_PROJECT_ID`
    * `BIGTABLE_INSTANCE_ID`
    * `BIGTABLE_TABLE_ID`

3.  **Run the Dataflow Job**
    Execute the script from your terminal. The `exec:java` command will compile your code, resolve dependencies, and launch the Dataflow job.

    ```bash
    .scripts/run_dataflow.sh
    ```

    This script handles all the necessary arguments, including JVM flags for the Java Module System, to ensure a smooth deployment.

4.  **Monitor the Job**
    After running the script, a new streaming Dataflow job will appear in the Google Cloud Console. You can monitor its progress, throughput, and logs from the Dataflow UI.

---

## Troubleshooting

* **`java.lang.NoClassDefFoundError`**: This typically indicates a missing dependency. Ensure your `pom.xml` is correct and run `mvn clean install` to rebuild the project.

* **`java.lang.IllegalAccessException`**: This is a Java Module System issue. The provided script includes the necessary `--add-opens` JVM flags to resolve this.

* **`IllegalArgumentException: Illegal base64 character`**: This means the data being processed is not valid Base64. Verify that your source pipeline is correctly encoding the data before publishing to Pub/Sub.

For more detailed information on the pipeline's logic, refer to the `technical_design_doc.md` file in this repository.
