

package io.mehitabel.bqstreamwriter

import io.mehitabel.bqstreamwriter.JsonWriterDefaultStream as JWS

fun main(args: Array<String>) {
    if (args.size < 4) {
        println("Arguments: project, dataset, table, source_file")
        return
    }
    val projectId = args[0]
    val datasetName = args[1]
    val tableName = args[2]
    val dataFile = args[3]

    JWS.run(projectId, datasetName, tableName, dataFile)
}
