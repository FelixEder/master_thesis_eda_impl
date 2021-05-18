package systems.systemD

import com.google.gson.Gson
import com.google.gson.stream.JsonReader
import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import systems.systemD.dataclasses.Employee
import systems.systemD.dataclasses.EmployerPayslip
import util.logging.CsvLogger
import util.logging.TwoHeaderCsvLogger
import java.io.File
import java.io.InputStream
import java.io.InputStreamReader
import java.net.BindException
import java.net.ConnectException
import java.nio.file.Files
import java.nio.file.Paths
import java.util.concurrent.LinkedBlockingQueue
import kotlin.random.Random

/**
 * A service that represents the employer in this scenario. Allows the employer to check an
 * IAVRegistry and send in a payslip.
 *
 * @author Felix Eder
 * @date 2021-04-20
 */

private val gson = Gson()
private val random = Random(123456789)
private val csvLogger = CsvLogger("src/main/kotlin/csv/systemD/database.csv",
    "src/main/kotlin/csv/systemD/incomingHttp.csv",
    "src/main/kotlin/csv/systemD/outgoingHttp.csv")
private val requestFailedLogger = TwoHeaderCsvLogger("src/main/kotlin/csv/systemD/requestFailedLogger.csv",
    "description", "time (ms)")

fun main(args: Array<String>) {
    val numberOfEmployees: Int = args[0].toInt()
    val numberOfEmployers: Int = args[1].toInt()

    val payslipQueue = LinkedBlockingQueue<MutableList<EmployerPayslip>>()

    GlobalScope.launch {
        sendInPayslip(payslipQueue)
    }

    while (true) {
        when(readLine()) {
            "checkCert" -> {
                println("Please enter IAV Certificate id")
                val certId = readLine()?.toInt() as Int
                println("Please enter personal number")
                val personalNumber = readLine() as String
                GlobalScope.launch {
                    println("Checking certificate against database.")
                    val result = checkIAVRegistry(personalNumber, certId)
                    if (result)
                        println("The certificate with id: $certId is valid")
                    else
                        println("The certificate with id: $certId is not valid")
                }
            }
            "payslip" -> {
                val startTime = System.currentTimeMillis()

                val stringBuilder = StringBuilder()

                stringBuilder.appendLine("Start Scenario 3a from employer")
                stringBuilder.appendLine("Start time (ms): $startTime")
                stringBuilder.appendLine("Number of employers: $numberOfEmployers")
                stringBuilder.appendLine("Number of employees per employer: $numberOfEmployees")

                File("src/main/kotlin/csv/summaryScenario3aStart.txt").writeText(stringBuilder.toString())

                for (i in 1..numberOfEmployers) {
                    GlobalScope.launch {
                        startEmployer(payslipQueue, i, numberOfEmployees)
                    }
                }
            }
        }
    }
}

/**
 * Starts a new employee process and sends in payslips for a certain moment.
 *
 * @param employerId The id of the employer.
 * @param numberOfEmployees The number of employees at the company.
 */
private fun startEmployer(payslipQueue: LinkedBlockingQueue<MutableList<EmployerPayslip>>, employerId: Int, numberOfEmployees: Int) {
    val numberOfMonths = 1 //For testing purposes, can be changed later
    val year = 2020 //For testing purposes, can be changed later

    for (month in 1..numberOfMonths) {
        val inputStream: InputStream = Files.newInputStream(Paths.get("src/main/kotlin/services/employer/resources/employeeData$employerId.json"))
        val reader = JsonReader(InputStreamReader(inputStream))
        reader.beginArray()

        val payslips = mutableListOf<EmployerPayslip>()

        for (i in 1..numberOfEmployees) {
            val employee: Employee = gson.fromJson(reader, Employee::class.java)
            val income = (0..40000).random()

            val payslip = EmployerPayslip(employerId, employee.personalNumber, employee.hasCert,
                employee.certId, year, month, income)
            payslips.add(payslip)
        }

        payslipQueue.add(payslips)
        println("Prepared payslips to be sent in for employer: $employerId")
    }
}

/**
 * Checks with the IAVControl system if a given certificate is valid.
 *
 * @param personalNumber The personal number to check for.
 * @param certId The id of the certificate to look up.
 * @return True if it is valid and vice versa.
 */
private suspend fun checkIAVRegistry(personalNumber: String, certId: Int): Boolean {
    val client = HttpClient(CIO)

    val startTime: Long = System.currentTimeMillis()
    val response: HttpResponse = client.request("http://localhost:4570/iav/checkCert?personalNumber=$personalNumber&certId=$certId") {
        headers {
            append("Accept", "application/json")
        }
        method = HttpMethod.Get
    }
    val stopTime: Long = System.currentTimeMillis()
    client.close()

    csvLogger.logOutgoingHttpRequest("GET", "Check if certificate is valid in IAVSystem",
        response.contentLength(), startTime, stopTime)

    return response.readText().toBoolean()
}

/**
 * Sends in the payslips to system D.
 *
 * @param payslips The payslips to send in.
 */
private suspend fun sendInPayslip(payslipQueue: LinkedBlockingQueue<MutableList<EmployerPayslip>>) {
    val client = HttpClient(CIO)
    var waitTime: Long = 500

    while (true) {
        val payslips = payslipQueue.take()

        delay(waitTime) //Delay some time in between just to not send in to many requests at a time and simulate "real life".
        println("Delayed with time: $waitTime")
        if (waitTime == 1000L) waitTime = 500
        var requestFailedTimeStart = 0L
        val startTime: Long = System.currentTimeMillis()

        while (true) {
            try {
                if (random.nextInt(0, 100) < 3)
                    throw ConnectException()
                val response: HttpResponse = client.post("http://localhost:4571/systemD/payslip") {
                    headers {
                        append("Accept", "application/json")
                    }
                    method = HttpMethod.Post
                    contentType(ContentType.Application.Json)
                    body = gson.toJson(payslips)
                }
                val stopTime: Long = System.currentTimeMillis()

                if (requestFailedTimeStart != 0L) {
                    requestFailedLogger.log("Employer payslip request failed, needed to wait", stopTime - requestFailedTimeStart)
                }

                println("Sent in payslip ${payslipQueue.size}")
                csvLogger.logOutgoingHttpRequest("POST", "Send payslips to system D",
                    response.contentLength(),  startTime, stopTime)
                waitTime += 100
                break
            } catch (connectException: ConnectException) {
                if (requestFailedTimeStart == 0L)
                    requestFailedTimeStart = System.currentTimeMillis()
                println("Connect exception to system D, trying again in 1 second")
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