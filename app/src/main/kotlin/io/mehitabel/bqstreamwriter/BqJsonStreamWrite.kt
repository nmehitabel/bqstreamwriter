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
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.flow.Flow
import arrow.fx.coroutines.*
import org.json.JSONArray
import org.json.JSONObject
import java.io.*
import java.util.concurrent.atomic.AtomicInteger

object JsonWriterDefaultStream {

    val batchCount = AtomicInteger()

    suspend fun run(
        projectId: String,
        datasetName: String,
        tableName: String,
        dataFile: String
    ) {

        val bigQueryService: BigQuery = BigQueryOptions.getDefaultInstance().getService()
        createDestinationTable(bigQueryService, projectId, datasetName, tableName)

        val client = BigQueryWriteClient.create()
        val writer = createDefaultStream(bigQueryService, client, projectId, datasetName, tableName)
//        val streamName = writer.streamName
        batchWrite(writer, dataFile)
            .onCompletion {
                if (!client.isShutdown) {
                    client.close()
                }
            }
            .collect {
                println(it.rowErrorsCount)
            }
//        client.close()
        println("${batchCount.get()} Batches written")

    }

    @OptIn(FlowPreview::class)
    suspend fun batchWrite(jsw: JsonStreamWriter, sourcePath: String): Flow<AppendRowsResponse> =
        jsonStreamWriter(jsw).flatMapConcat { jsWriter ->
            getFileDataBatchedJson(sourcePath)
                .parMapUnordered(concurrency = 2) { chunk ->
                     coroutineScope {
                         val future: ApiFuture<AppendRowsResponse> = jsWriter.append(JSONArray(chunk))
                         future.await()
//                         ApiFutures.addCallback(future, object : ApiFutureCallback<AppendRowsResponse> {
//                             override fun onFailure(t: Throwable) {
//                                 throw t
//                             }
//                             override fun onSuccess(result: AppendRowsResponse) {
//                                 batchCount.incrementAndGet()
//                                 result
//                             }
//                        }, MoreExecutors.directExecutor())
                        //future.get()
                     }
                }
        }
    fun jsonStreamWriter(jsWriter: JsonStreamWriter): Flow<JsonStreamWriter> = flow {
        jsWriter.use { emit(it)}
    }


    fun InputStream.linesToFlow(): Flow<String> =
        flow {
            bufferedReader()
                .useLines { lines ->
                    lines.forEach { line -> emit(line) }
                }
        }

    fun getFileDataBatchedJson(dataFile: String): Flow<List<JSONObject>> {
        val reader: InputStream = FileInputStream(File(dataFile))
        return reader
            .linesToFlow()
            .map { it -> JSONObject(it) }
            .chunked(100)
            .flowOn(Dispatchers.IO)
    }

    // createDestinationTable: Creates the destination table for streaming with the desired schema.
    fun createDestinationTable(
        service: BigQuery,
        projectId: String, datasetName: String, tableName: String
    ) {
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
        val table: Table? = service.getTable(tableId)
        if (table == null) {
            val tableInfo: TableInfo = TableInfo.newBuilder(tableId, StandardTableDefinition.of(schema))
                .setExpirationTime(86400 * 4)
                .build()
            service.create(tableInfo)
        }
    }


    fun createDefaultStream(
        service: BigQuery,
        writeClient: BigQueryWriteClient,
        projectId: String,
        datasetName: String,
        tableName: String
    ): JsonStreamWriter {
        // Get the schema of the destination table and convert to the equivalent BigQueryStorage type.
        val table: Table = service.getTable(datasetName, tableName)
        val schema: Schema? = table.getDefinition<StandardTableDefinition>().getSchema()
        val tableSchema: TableSchema = BqToBqStorageSchemaConverter.convertTableSchema(schema!!) // force npe if null
        val parentTable: TableName = TableName.of(projectId, datasetName, tableName)
        return JsonStreamWriter.newBuilder(parentTable.toString(), tableSchema, writeClient).build()
    }

}

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