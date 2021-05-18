package systems.iavSystem

import com.google.gson.Gson
import systems.systemB.SystemBDBManager
import systems.iavSystem.database.PSQLDBManager
import systems.iavSystem.iavControl.IAVControl
import systems.iavSystem.stickControl.StickControl
import systems.systemF.dataclasses.MonthlyIncome
import spark.Spark.*
import util.logging.CsvLogger
import util.logging.EventLogger
import util.logging.TwoHeaderCsvLogger
import java.util.concurrent.LinkedBlockingQueue

private val iavCsvLogger = CsvLogger("src/main/kotlin/csv/iavSystem/database.csv",
    "src/main/kotlin/csv/iavSystem/incomingHttp.csv",
    "src/main/kotlin/csv/iavSystem/outgoingHttp.csv")

private val systemBCsvLogger = CsvLogger("src/main/kotlin/csv/systemB/database.csv",
    "src/main/kotlin/csv/systemB/incomingHttp.csv",
    "src/main/kotlin/csv/systemB/outgoingHttp.csv")

private val iavControlLogger = TwoHeaderCsvLogger("src/main/kotlin/csv/iavSystem/iavPersonalCheck.csv",
    "personalNumber", "time (ms)")

private val stickControlLogger = TwoHeaderCsvLogger("src/main/kotlin/csv/iavSystem/stickControlLog.csv",
    "personalNumber", "time (ms)")

private val eventLogger = EventLogger("src/main/kotlin/csv/iavSystem/events.csv")
private val gson = Gson()
/**
 * Main file of the project, here everything is started.
 *
 * @author Felix Eder
 * @date 2021-04-02
 */
fun main() {
    val psqlDbManager = PSQLDBManager(iavCsvLogger)
    val systemBDbManager = SystemBDBManager(systemBCsvLogger)
    val iavControl = IAVControl(psqlDbManager, systemBDbManager, iavCsvLogger, iavControlLogger, eventLogger)
    val stickControl = StickControl(psqlDbManager, iavCsvLogger, stickControlLogger, eventLogger)
    val monthlyIncomeQueue = LinkedBlockingQueue<MonthlyIncome>()

    setUpHttpServer(psqlDbManager, iavControl, stickControl, monthlyIncomeQueue)

    handleIncomingIncomes(stickControl, monthlyIncomeQueue)

    while (true) {
        when(readLine()) {

            "log" -> {
                iavCsvLogger.writeSummaryToConsole()
            }

            "close" -> {
                iavCsvLogger.close()
            }
            else ->
                println("Unknown input")
        }
    }
}

/**
 * Sets up a simple HTTP server that checks if a certificate exists in the registry.
 *
 * @param psqldbManager The database manager for the IAV registry.
 */
private fun setUpHttpServer(psqldbManager: PSQLDBManager, iavControl: IAVControl, stickControl: StickControl,
                            queue: LinkedBlockingQueue<MonthlyIncome>) {
    port(4570)

    get("/iav/checkCert") { req, res ->
        iavCsvLogger.logIncomingHttpRequest("GET", "Request to check certificate authenticity",
                req.contentLength())

        val personalNumber: String = req.queryParams("personalNumber").toString()
        val certId: Int = req.queryParams("certId").toInt()
        res.status(200)
        psqldbManager.doesCertificateExist(personalNumber, certId)
    }

    post("iav/personEntitled") { req, res ->
        //TODO Figure out what kind of event logging needs to come here.

        val packageToIAV = gson.fromJson(req.body(), PackageToIAV::class.java)

        iavControl.makeIAVCheck(packageToIAV)

        res.status(200)
        "ok"
    }

    post("iav/monthlyIncomeReported") { req, res ->
        //TODO Figure out what kind of event logging needs to come here.

        val monthlyIncome: MonthlyIncome = gson.fromJson(req.body(), MonthlyIncome::class.java)

        queue.add(monthlyIncome)

        res.status(200)
        "ok"
    }
}

fun handleIncomingIncomes(stickControl: StickControl, queue: LinkedBlockingQueue<MonthlyIncome>) {
    while (true) {
        val monthlyIncome = queue.take()
        stickControl.startSingleStickControl(monthlyIncome)
    }
}
