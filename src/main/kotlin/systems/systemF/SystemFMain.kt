package systems.systemF

import com.google.gson.Gson
import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import systems.systemF.dataclasses.MonthlyIncome
import spark.Spark.*
import util.logging.CsvLogger
import util.logging.EventLogger
import java.io.File
import java.net.BindException
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicInteger

/**
 * Simple web service that handles connection with the monthly income database and sets up a
 * REST api in order to send out the information.
 *
 * @author Felix Eder
 * @date 2021-04-17
 */

private val csvLogger = CsvLogger("src/main/kotlin/csv/systemF/database.csv",
    "src/main/kotlin/csv/systemF/incomingHttp.csv",
    "src/main/kotlin/csv/systemF/outgoingHttp.csv")
private val eventLogger = EventLogger("src/main/kotlin/csv/systemF/events.csv")
private val systemFDBManager = SystemFDBManager(csvLogger)
private val gson = Gson()
private const val numberOfEmployers = 10000 //Used for logging purposes

/**
 * Main function that calls other functions to set up the web service.
 */
fun main(args: Array<String>) {
    val year: Int = args[0].toInt()
    val month: Int = args[1].toInt()
    val concurrentQueue = LinkedBlockingQueue<List<MonthlyIncome>>()

    setUpHttpServer(year, month, concurrentQueue)

    GlobalScope.launch {
        handleIncomeQueue(concurrentQueue)
    }
}

/**
 * Sets up a simple HTTP server that listens for GET requests and sends over monthly income data.
 */
private fun setUpHttpServer(systemFYear: Int, systemFMonth: Int, concurrentQueue: LinkedBlockingQueue<List<MonthlyIncome>>) {
    port(4569)

    post("systemF/postIncome") { req, res ->
        csvLogger.logIncomingHttpRequest("POST",
            "Incoming request with income data from system E", req.contentLength())

        val incomes: List<MonthlyIncome> = gson.fromJson(req.body(), Array<MonthlyIncome>::class.java).toList()

        concurrentQueue.add(incomes)
        res.status(200)
        "ok"
    }

    get("systemF/income") { req, res ->
        csvLogger.logIncomingHttpRequest("GET",
            "Request to get monthly income from systemF service", req.contentLength())

        val personalNumber: String = req.queryParams("personalNumber").toString()
        val year: Int = req.queryParams("year").toInt()
        val month: Int = req.queryParams("month").toInt()

        systemFDBManager.getMonthlyIncome(personalNumber, year, month)
    }
}

private suspend fun handleIncomeQueue(queue: LinkedBlockingQueue<List<MonthlyIncome>>) {
    val payslipsReceived = AtomicInteger()
    payslipsReceived.set(0)
    val client = HttpClient(CIO)

    while (true) {
        val incomes = queue.take()
        val payslips = payslipsReceived.incrementAndGet()
        systemFDBManager.bulkInsertMonthlyIncome(incomes)
        println("Inserted monthly incomes for employer: ${incomes[0].employerId}")
        checkForIavPersons(incomes, client)

        if (payslips >= numberOfEmployers) {
            val stopTime = System.currentTimeMillis()
            println("Have gotten all the payslips this salary period")

            val stringBuilder = StringBuilder()

            stringBuilder.appendLine("Stop Scenario 3a from employer")
            stringBuilder.appendLine("Stop time (ms): $stopTime")

            File("src/main/kotlin/csv/summaryScenario3aEnd.txt").writeText(stringBuilder.toString())

            break
        }
    }

    client.close()
}

private suspend fun checkForIavPersons(incomes: List<MonthlyIncome>, client: HttpClient) {
    for (income in incomes) {
        if (income.hasCert) {
            sendIavPersonReportedEvent(income, client)
        }
    }
}

/**
 * Sends an event to the IAVSystem that an IAV person has reported a new monthly income.
 *
 * @param monthlyIncome The monthly income to send an event for.
 */
private suspend fun sendIavPersonReportedEvent(monthlyIncome: MonthlyIncome, client: HttpClient) {
    while (true) {
        try {
            val startTime = System.currentTimeMillis()
            val response: HttpResponse = client.request("http://localhost:4570/iav/monthlyIncomeReported") {
                headers {
                    append("Accept", "application/json")
                }
                method = HttpMethod.Post
                contentType(ContentType.Application.Json)
                body = gson.toJson(monthlyIncome)
            }
            val stopTime = System.currentTimeMillis()

            eventLogger.logSentEvent("Sent event that an IAV person: ${monthlyIncome.personalNumber} har reported a new monthly income")
            break
        } catch (exception: BindException) {
            println("Bind exception: $exception")
            delay(100)
            continue
        }
    }

}