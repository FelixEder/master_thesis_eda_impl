package systems.systemE

import com.google.gson.Gson
import com.google.gson.JsonArray
import com.google.gson.JsonParser
import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import systems.systemE.dataclasses.SystemEIavPayslip
import systems.systemF.dataclasses.MonthlyIncome
import spark.Spark.port
import spark.Spark.post
import util.logging.CsvLogger
import util.logging.EventLogger
import java.net.BindException
import java.net.ConnectException
import java.util.concurrent.LinkedBlockingQueue

/**
 * A simple web service that gets monthly income reports from employers (system D).
 *
 * @author Felix Eder
 * @date 2021-04-20
 */

private val gson = Gson()
private val eventLogger = EventLogger("src/main/kotlin/csv/systemE/events.csv")
private val csvLogger = CsvLogger("src/main/kotlin/csv/systemE/database.csv",
    "src/main/kotlin/csv/systemE/incomingHttp.csv",
    "src/main/kotlin/csv/systemE/outgoingHttp.csv")

fun main() {
    val blockingQueue = LinkedBlockingQueue<List<SystemEIavPayslip>>()
    setUpHttpServer(blockingQueue)
    GlobalScope.launch {
        sendInfoToSystemF(blockingQueue)
    }
}

/**
 * Sets up a simple HTTP server that listens for POST-requests from employers.
 */
private fun setUpHttpServer(blockingQueue: LinkedBlockingQueue<List<SystemEIavPayslip>>) {
    port(4571)

    post("/systemE/payslip") { req, res ->
        csvLogger.logIncomingHttpRequest("POST",
            "Incoming post request with employer payslips",
            req.contentLength())

        val jsonArray: JsonArray = JsonParser.parseString(req.body()).asJsonArray
        val employerPayslips = mutableListOf<SystemEIavPayslip>()

        for (jsonElement in jsonArray) {
            val jsonObject = jsonElement.asJsonObject

            val employerId: Int = jsonObject.get("employerId").asInt
            val personalNumber: String = jsonObject.get("personalNumber").asString
            val hasCert: Boolean = jsonObject.get("hasCert").asBoolean
            val certId: Int = jsonObject.get("certId").asInt
            val year: Int = jsonObject.get("year").asInt
            val month: Int = jsonObject.get("month").asInt
            val income: Int = jsonObject.get("income").asInt

            val payslip = SystemEIavPayslip(employerId, personalNumber, hasCert, certId, year, month, income)
            employerPayslips.add(payslip)
            println("Calculated taxes for person: ${payslip.personalNumber}")
        }
        blockingQueue.add(employerPayslips)
        println("Calculated taxes for all employees at employer ${employerPayslips[0].employerId}")

        res.status(200)
        "ok"
    }

    post("/systemE/certificateInvalidEvent") { req, res ->
        //TODO Figure out what kind of event logging needs to come here.

        val monthlyIncome: MonthlyIncome = gson.fromJson(req.body(), MonthlyIncome::class.java)

        val stringBuilder = StringBuilder()

        stringBuilder.appendLine("Person ${monthlyIncome.personalNumber} was marked as IAV but no certificate was found.")
        stringBuilder.appendLine("Please update the taxes for this person.")
        stringBuilder.appendLine("*Updating taxes for ${monthlyIncome.personalNumber}*")
        println(stringBuilder.toString())

        res.status(200)
        "ok"
    }
}

/**
 * Sends the personal payslip information over to system F.
 *
 * @param blockingQueue The queue with data to send over.
 */
private suspend fun sendInfoToSystemF(blockingQueue: LinkedBlockingQueue<List<SystemEIavPayslip>>) {
    val client = HttpClient(CIO)

    while (true) {
        val employerPayslips = blockingQueue.take()
        println("Sending in payslips for employer ${employerPayslips.get(0).employerId} to system F")
        val startTime: Long = System.currentTimeMillis()

        while (true) {
            try {
                val response: HttpResponse = client.post("http://localhost:4569/systemF/postIncome") {
                    headers {
                        append("Accept", "application/json")
                    }
                    method = HttpMethod.Post
                    contentType(ContentType.Application.Json)
                    body = gson.toJson(employerPayslips)
                }
                val stopTime: Long = System.currentTimeMillis()

                eventLogger.logSentEvent("Sent event that employer payslip has been calculated")
                break
            } catch (exception: ConnectException) {
                println("Connect exception to System F, trying again in 1 second")
                delay(1000)
                continue
            } catch (bindException: BindException) {
                println(bindException)
                delay(100)
                continue
            }
        }
    }
    client.close()
}