package util.logging

import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter
import java.io.File

/**
 * Super simple CSV printer class with two headers.
 *
 * @author Felix Eder
 * @date 2021-05-01
 */
class TwoHeaderCsvLogger(filePathString: String, header1: String, header2:String) {

    private val csvPrinter = CSVPrinter(File(filePathString).bufferedWriter(),
        CSVFormat.DEFAULT.withHeader(header1, header2))

    fun log(value1: Any, value2: Any) {
        synchronized(this) {
            csvPrinter.printRecord(value1, value2)
            csvPrinter.flush()
        }
    }

    fun close() {
        csvPrinter.close()
    }
}