package io.mehitabel.bqstreamwriter


/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.google.api.core.ApiFuture
import com.google.api.core.ApiFutureCallback
import com.google.api.core.ApiFutures
import com.google.cloud.bigquery.*
import com.google.cloud.bigquery.storage.v1.*
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient
import com.google.common.util.concurrent.MoreExecutors
//import com.google.protobuf.Descriptors.DescriptorValidationException
//import org.jetbrains.kotlin.protobuf.Descriptors.DescriptorValidationException
import org.json.JSONArray
import org.json.JSONObject
import java.io.BufferedReader
import java.io.FileReader
import java.io.IOException
import java.util.concurrent.atomic.AtomicInteger


object JsonWriterDefaultStream {
    fun run(projectId: String,
            datasetName: String,
            tableName: String,
            dataFile: String) {
        createDestinationTable(projectId, datasetName, tableName)
        writeToDefaultStream(projectId, datasetName, tableName, dataFile)
    }

    // createDestinationTable: Creates the destination table for streaming with the desired schema.
    fun createDestinationTable(
        projectId: String, datasetName: String, tableName: String
    ) {
        val bigquery: BigQuery = BigQueryOptions.getDefaultInstance().getService()
        // Create a schema that matches the source data.
        val schema: Schema = Schema.of(
            Field.of("commit", StandardSQLTypeName.STRING),
            Field.newBuilder("parent", StandardSQLTypeName.STRING)
                .setMode(Field.Mode.REPEATED)
                .build(),
            Field.of("author", StandardSQLTypeName.STRING),
            Field.of("committer", StandardSQLTypeName.STRING),
            Field.of("commit_date", StandardSQLTypeName.DATETIME),
            Field.of(
                "commit_msg",
                StandardSQLTypeName.STRUCT,
                FieldList.of(
                    Field.of("subject", StandardSQLTypeName.STRING),
                    Field.of("message", StandardSQLTypeName.STRING)
                )
            ),
            Field.of("repo_name", StandardSQLTypeName.STRING)
        )

        // Create a table that uses this schema.
        val tableId: TableId = TableId.of(projectId, datasetName, tableName)
        val table: Table? = bigquery.getTable(tableId)
        if (table == null) {
            val tableInfo: TableInfo = TableInfo.newBuilder(tableId, StandardTableDefinition.of(schema)).build()
            bigquery.create(tableInfo)
        }
    }

    // writeToDefaultStream: Writes records from the source file to the destination table.
    @Throws(
        //org.jetbrains.kotlin.protobuf.Descriptors.DescriptorValidationException::class,
        InterruptedException::class,
        IOException::class
    )
    fun writeToDefaultStream(
        projectId: String, datasetName: String, tableName: String, dataFile: String
    ) {
        val bigquery: BigQuery = BigQueryOptions.getDefaultInstance().getService()
        // Get the schema of the destination table and convert to the equivalent BigQueryStorage type.
        val table: Table = bigquery.getTable(datasetName, tableName)
        val schema: Schema? = table.getDefinition<StandardTableDefinition>().getSchema()
        val tableSchema: TableSchema = BqToBqStorageSchemaConverter.convertTableSchema(schema!!) // force npe if null
        val batchCount = AtomicInteger()
        // Use the JSON stream writer to send records in JSON format.
        val parentTable: TableName = TableName.of(projectId, datasetName, tableName)

        val client = BigQueryWriteClient.create()

        JsonStreamWriter.newBuilder(parentTable.toString(), tableSchema, client).build().use { writer ->
            // Read JSON data from the source file and send it to the Write API.
            val reader = BufferedReader(FileReader(dataFile))
            var line = reader.readLine()
            while (line != null) {
                // As a best practice, send batches of records, instead of single records at a time.
                val jsonArr = JSONArray()
                for (i in 0..99) {
                    val record = JSONObject(line)
                    jsonArr.put(record)
                    line = reader.readLine()
                    if (line == null) {
                        break
                    }
                } // batch
                val future: ApiFuture<AppendRowsResponse> = writer.append(jsonArr)
                // The append method is asynchronous. Rather than waiting for the method to complete,
                // which can hurt performance, register a completion callback and continue streaming.
                ApiFutures.addCallback(future, object : ApiFutureCallback<AppendRowsResponse> {
                    override fun onFailure(t: Throwable) {
                        throw t
                    }

                    override fun onSuccess(result: AppendRowsResponse) {
                        batchCount.incrementAndGet()
                    }

                }, MoreExecutors.directExecutor())
            }
        }

        println("${batchCount.get()} Batches written")
    }
}

//internal class AppendCompleteCallback : ApiFutureCallback<AppendRowsResponse?> {
//    fun onSuccess(response: AppendRowsResponse) {
//        synchronized(lock) {
//            if (response.hasError()) {
//                System.out.format("Error: %s\n", response.getError().toString())
//            } else {
//                ++batchCount
//                System.out.format("Wrote batch %d\n", batchCount)
//            }
//        }
//    }
//
//    fun onFailure(throwable: Throwable) {
//        System.out.format("Error: %s\n", throwable.toString())
//    }

//    companion object {
//        private var batchCount = 0
//        private val lock = Any()
//    }
//}

private object BqToBqStorageSchemaConverter {
    private val BQTableSchemaModeMap = mapOf(
        Field.Mode.NULLABLE to TableFieldSchema.Mode.NULLABLE,
        Field.Mode.REPEATED to TableFieldSchema.Mode.REPEATED,
        Field.Mode.REQUIRED to TableFieldSchema.Mode.REQUIRED,
    )
    private val BQTableSchemaTypeMap = mapOf(
        StandardSQLTypeName.BOOL to TableFieldSchema.Type.BOOL,
        StandardSQLTypeName.BYTES to TableFieldSchema.Type.BYTES,
        StandardSQLTypeName.DATE to TableFieldSchema.Type.DATE,
        StandardSQLTypeName.DATETIME to TableFieldSchema.Type.DATETIME,
        StandardSQLTypeName.FLOAT64 to TableFieldSchema.Type.DOUBLE,
        StandardSQLTypeName.GEOGRAPHY to TableFieldSchema.Type.GEOGRAPHY,
        StandardSQLTypeName.INT64 to TableFieldSchema.Type.INT64,
        StandardSQLTypeName.NUMERIC to TableFieldSchema.Type.NUMERIC,
        StandardSQLTypeName.STRING to TableFieldSchema.Type.STRING,
        StandardSQLTypeName.STRUCT to TableFieldSchema.Type.STRUCT,
        StandardSQLTypeName.TIME to TableFieldSchema.Type.TIME,
        StandardSQLTypeName.TIMESTAMP to TableFieldSchema.Type.TIMESTAMP,
    )

    fun convertTableSchema(schema: Schema): TableSchema =
        schema.fields.foldIndexed(initial = TableSchema.newBuilder()) { index, builder, field ->
            builder.addFields(index, convertFieldSchema(field))
        }.build()

    private fun convertFieldSchema(field: Field): TableFieldSchema = TableFieldSchema.newBuilder()
        .apply {
            mode = BQTableSchemaModeMap[field.mode ?: Field.Mode.NULLABLE]
            name = field.name
            type = BQTableSchemaTypeMap[field.type.standardType]
            field.description?.let { this.description = it }
            field.subFields?.forEachIndexed { i, f -> addFields(i, convertFieldSchema(f)) }
        }
        .build()
}