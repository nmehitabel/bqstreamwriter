package io.mehitabel.bqstreamwriter

import com.google.api.core.ApiFutureCallback
import com.google.api.core.ApiFutures
import com.google.cloud.bigquery.storage.v1.*
import com.google.common.util.concurrent.MoreExecutors
import com.google.protobuf.Descriptors
import org.json.JSONArray
import org.json.JSONObject
import java.io.IOException
import java.util.concurrent.ExecutionException
import java.util.concurrent.Phaser
import javax.annotation.concurrent.GuardedBy


object WritePendingStream {
    @Throws(
        Descriptors.DescriptorValidationException::class,
        InterruptedException::class,
        IOException::class
    )
    fun runWritePendingStream() {
        // TODO(developer): Replace these variables before running the sample.
        val projectId = "MY_PROJECT_ID"
        val datasetName = "MY_DATASET_NAME"
        val tableName = "MY_TABLE_NAME"
        writePendingStream(projectId, datasetName, tableName)
    }

    @Throws(
        Descriptors.DescriptorValidationException::class,
        InterruptedException::class,
        IOException::class
    )
    fun writePendingStream(projectId: String?, datasetName: String?, tableName: String?) {
        val client = BigQueryWriteClient.create()
        val parentTable = TableName.of(projectId, datasetName, tableName)
        val writer = DataWriter()
        // One time initialization.
        writer.initialize(parentTable, client)
        try {
            // Write two batches of fake data to the stream, each with 10 JSON records.  Data may be
            // batched up to the maximum request size:
            // https://cloud.google.com/bigquery/quotas#write-api-limits
            var offset: Long = 0
            for (i in 0..1) {
                // Create a JSON object that is compatible with the table schema.
                val jsonArr = JSONArray()
                for (j in 0..9) {
                    val record = JSONObject()
                    record.put("col1", String.format("batch-record %03d-%03d", i, j))
                    jsonArr.put(record)
                }
                writer.append(jsonArr, offset)
                offset += jsonArr.length().toLong()
            }
        } catch (e: ExecutionException) {
            // If the wrapped exception is a StatusRuntimeException, check the state of the operation.
            // If the state is INTERNAL, CANCELLED, or ABORTED, you can retry. For more information, see:
            // https://grpc.github.io/grpc-java/javadoc/io/grpc/StatusRuntimeException.html
            println("Failed to append records. \n$e")
        }

        // Final cleanup for the stream.
        writer.cleanup(client)
        println("Appended records successfully.")

        // Once all streams are done, if all writes were successful, commit all of them in one request.
        // This example only has the one stream. If any streams failed, their workload may be
        // retried on a new stream, and then only the successful stream should be included in the
        // commit.
        val commitRequest = BatchCommitWriteStreamsRequest.newBuilder()
            .setParent(parentTable.toString())
            .addWriteStreams(writer.streamName)
            .build()
        val commitResponse = client.batchCommitWriteStreams(commitRequest)
        // If the response does not have a commit time, it means the commit operation failed.
        if (commitResponse.hasCommitTime() == false) {
            for (err in commitResponse.streamErrorsList) {
                println(err.errorMessage)
            }
            throw RuntimeException("Error committing the streams")
        }
        println("Appended and committed records successfully.")
    }

    // A simple wrapper object showing how the stateful stream writer should be used.
    private class DataWriter {
        private var streamWriter: JsonStreamWriter? = null

        // Track the number of in-flight requests to wait for all responses before shutting down.
        private val inflightRequestCount = Phaser(1)
        private val lock = Any()

        @GuardedBy("lock")
        private var error: RuntimeException? = null

        @Throws(
            IOException::class,
            Descriptors.DescriptorValidationException::class,
            InterruptedException::class
        )
        fun initialize(parentTable: TableName, client: BigQueryWriteClient) {
            // Initialize a write stream for the specified table.
            // For more information on WriteStream.Type, see:
            // https://googleapis.dev/java/google-cloud-bigquerystorage/latest/com/google/cloud/bigquery/storage/v1/WriteStream.Type.html
            val stream = WriteStream.newBuilder().setType(WriteStream.Type.PENDING).build()
            val createWriteStreamRequest = CreateWriteStreamRequest.newBuilder()
                .setParent(parentTable.toString())
                .setWriteStream(stream)
                .build()
            val writeStream = client.createWriteStream(createWriteStreamRequest)

            // Use the JSON stream writer to send records in JSON format.
            // For more information about JsonStreamWriter, see:
            // https://googleapis.dev/java/google-cloud-bigquerystorage/latest/com/google/cloud/bigquery/storage/v1beta2/JsonStreamWriter.html
            streamWriter = JsonStreamWriter.newBuilder(writeStream.name, writeStream.tableSchema).build()
        }

        @Throws(Descriptors.DescriptorValidationException::class, IOException::class, ExecutionException::class)
        fun append(data: JSONArray?, offset: Long) {
            synchronized(lock) {
                // If earlier appends have failed, we need to reset before continuing.
                if (error != null) {
                    throw error!!
                }
            }
            // Append asynchronously for increased throughput.
            val future = streamWriter!!.append(data, offset)
            ApiFutures.addCallback(
                future, AppendCompleteCallback(this), MoreExecutors.directExecutor()
            )
            // Increase the count of in-flight requests.
            inflightRequestCount.register()
        }

        fun cleanup(client: BigQueryWriteClient) {
            // Wait for all in-flight requests to complete.
            inflightRequestCount.arriveAndAwaitAdvance()

            // Close the connection to the server.
            streamWriter!!.close()

            // Verify that no error occurred in the stream.
            synchronized(lock) {
                if (error != null) {
                    throw error!!
                }
            }

            // Finalize the stream.
            val finalizeResponse = client.finalizeWriteStream(
                streamWriter!!.streamName
            )
            println("Rows written: " + finalizeResponse.rowCount)
        }

        val streamName: String
            get() = streamWriter!!.streamName

        internal class AppendCompleteCallback(private val parent: DataWriter) :
            ApiFutureCallback<AppendRowsResponse> {
            override fun onSuccess(response: AppendRowsResponse) {
                System.out.format("Append %d success\n", response.appendResult.offset.value)
                done()
            }

            override fun onFailure(throwable: Throwable) {
                synchronized(parent.lock) {
                    if (parent.error == null) {
                        val storageException =
                            Exceptions.toStorageException(throwable)
                        parent.error =
                            storageException ?: RuntimeException(throwable)
                    }
                }
                System.out.format("Error: %s\n", throwable.toString())
                done()
            }

            private fun done() {
                // Reduce the count of in-flight requests.
                parent.inflightRequestCount.arriveAndDeregister()
            }
        }
    }
}
// [END bigquerystorage_jsonstreamwriter_pending]
