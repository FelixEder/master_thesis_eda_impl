package util.logging

import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter
import java.io.File
import java.lang.StringBuilder

class CsvLogger(databaseCsvFilePath: String, incomingHttpCsvFilepath: String, outgoingHttpCsvFilepath: String) {
    private var outgoingRequestsTotal = 0
    private var incomingRequestsTotal = 0

    private var databaseReads = 0
    private var databaseWrites = 0

    private val databaseCsvPrinter = CSVPrinter(File(databaseCsvFilePath).bufferedWriter(),
        CSVFormat.DEFAULT.withHeader("type", "description", "time (ms)"))

    private val incomingHttpCsvPrinter = CSVPrinter(File(incomingHttpCsvFilepath).bufferedWriter(),
        CSVFormat.DEFAULT.withHeader("type", "description", "size (bits)"))

    private val outgoingHttpCsvPrinter = CSVPrinter(File(outgoingHttpCsvFilepath).bufferedWriter(),
        CSVFormat.DEFAULT.withHeader("type", "description", "size (bits)", "time (ms)"))

    fun logIncomingHttpRequest(type: String, description: String, numberOfBytes: Int) {
        val numberOfBits = numberOfBytes * 8

        synchronized(this) { //Synchronized block in order to avoid race condition
            incomingHttpCsvPrinter.printRecord(type, description, numberOfBits)
            incomingHttpCsvPrinter.flush()
        }
    }

    fun logIncomingHttpRequest(type: String, description: String, numberOfBytes: Long?) {
        incomingRequestsTotal++

        var numberOfBits = 0

        if (numberOfBytes != null)
            numberOfBits = numberOfBytes.toInt() * 8

        synchronized(this) { //Synchronized block in order to avoid race condition
            incomingHttpCsvPrinter.printRecord(type, description, numberOfBits)
            incomingHttpCsvPrinter.flush()
        }
    }

    fun logOutgoingHttpRequest(type: String, description: String, numberOfBytes: Int, startTime: Long, stopTime: Long) {
        val requestTime = stopTime - startTime

        val numberOfBits = numberOfBytes * 8

        synchronized(this) { //Synchronized block in order to avoid race condition
            outgoingHttpCsvPrinter.printRecord(type, description, numberOfBits, requestTime)
            outgoingHttpCsvPrinter.flush()
        }
    }

    fun logOutgoingHttpRequest(type: String, description: String, numberOfBytes: Long?, startTime: Long, stopTime: Long) {
        outgoingRequestsTotal++
        val requestTime = stopTime - startTime

        var numberOfBits = 0

        if (numberOfBytes != null)
            numberOfBits = numberOfBytes.toInt() * 8

        synchronized(this) { //Synchronized block in order to avoid race condition
            outgoingHttpCsvPrinter.printRecord(type, description, numberOfBits, requestTime)
            outgoingHttpCsvPrinter.flush()
        }
    }

    fun logDatabaseAccess(type: String, description: String) {
        if (type == "READ")
            databaseReads++
        else
            databaseWrites++

        synchronized(this) {
            databaseCsvPrinter.printRecord(type, description, "no time recorded")
            databaseCsvPrinter.flush()
        }
    }

    /**
     * Logs a database access into a CSV-file.
     *
     * @param type The type of database access (READ or WRITE).
     * @param description What the access does.
     * @param startTime The time the database access started.
     * @param stopTime The time the database access stopped.
     */
    fun logDatabaseAccess(type: String, description: String, startTime: Long, stopTime: Long) {
        if (type == "READ")
            databaseReads++
        else
            databaseWrites++

        synchronized(this) {
            databaseCsvPrinter.printRecord(type, description, stopTime - startTime)
            databaseCsvPrinter.flush()
        }
    }
    fun writeSummaryToConsole() {
        println("Total number of outgoing http requests: $outgoingRequestsTotal")
        println("Total number of incoming http requests: $incomingRequestsTotal")
        println("Number of database reads: $databaseReads")
        println("Number of database writes: $databaseWrites")
    }

    fun getSummarizedString(): String {
        val stringBuilder = StringBuilder()

        stringBuilder.appendLine("Summary for CSV-logger:")
        stringBuilder.appendLine("Total number of outgoing http requests: $outgoingRequestsTotal")
        stringBuilder.appendLine("Total number of incoming http requests: $incomingRequestsTotal")
        stringBuilder.appendLine("Number of database reads: $databaseReads")
        stringBuilder.appendLine("Number of database writes: $databaseWrites")

        return stringBuilder.toString()
    }

    fun close() {
        databaseCsvPrinter.close()
        incomingHttpCsvPrinter.close()
        outgoingHttpCsvPrinter.close()
    }
}