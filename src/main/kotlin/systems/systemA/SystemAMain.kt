package systems.systemA

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import util.logging.CsvLogger
import util.logging.EventLogger
import java.io.File

/**
 * A simple web service that reads basic personal information from a file and sets up a simple
 * web api in order to get the information.
 *
 * @author Felix Eder
 * @date 2021-04-12
 */
private val csvLogger = CsvLogger("src/main/kotlin/csv/systemA/database.csv",
    "src/main/kotlin/csv/systemA/incomingHttp.csv",
    "src/main/kotlin/csv/systemA/outgoingHttp.csv")
private val eventLogger = EventLogger("src/main/kotlin/csv/systemA/events.csv")
private val systemALogic: SystemALogic = SystemALogic(csvLogger, eventLogger)

/**
 * Main function that calls other functions to set up the web service.
 */
fun main() {
    while (true) {
        when(readLine()) {
            "start" -> {
                GlobalScope.launch {
                    systemALogic.startIncomeCheck()
                }
            }
        }
    }
}
