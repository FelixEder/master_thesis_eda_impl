package util.logging

import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter
import java.io.File

class EventLogger(csvFilePath: String) {
    private var sentEventsTotal = 0

    private val csvPrinter = CSVPrinter(File(csvFilePath).bufferedWriter(),
        CSVFormat.DEFAULT.withHeader("description"))


    fun logSentEvent(description: String) {
        sentEventsTotal++

        synchronized(this) {
            csvPrinter.printRecord(description)
        }
    }
}