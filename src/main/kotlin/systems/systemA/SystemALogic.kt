package systems.systemA

import com.google.gson.Gson
import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import systems.systemA.dataclasses.PackageToIAV
import systems.systemA.dataclasses.FiveYearIncome
import util.logging.CsvLogger
import util.logging.EventLogger
import java.io.File

/**
 * Class that handles the underlying logic for the system A.
 *
 * @author Felix Eder
 * @date 2021-04.14
 */
class SystemALogic(private val csvLogger: CsvLogger, private val eventLogger: EventLogger) {
    private val systemADBManager: SystemADBManager = SystemADBManager(csvLogger)
    private var systemACounter = 0
    private val gson = Gson()

    suspend fun startIncomeCheck() {
        val incomes = systemADBManager.getAllIncomes()

        val scenarioStartTime = System.currentTimeMillis()
        File("src/main/kotlin/csv/systemA/summary.csv").writeText(
            "Start time for scenario 1: $scenarioStartTime ms")

        for (income in incomes) {
            val startTime = System.currentTimeMillis()
            println("Checking income for id: ${income.personalNumber}")
            checkIncome(income, startTime)
        }
    }

    private suspend fun checkIncome(fiveYearIncome: FiveYearIncome, startTime: Long) {
        /*
        val fiveYearIncome = systemADBManager.getIncomeById(incomeId)
        if (fiveYearIncome == null) {
            println("Income with id: $incomeId not found in db, aborting")
            return
        }
         */

        if (fiveYearIncome.salaryIncome <= 100000 && fiveYearIncome.capitalIncome <= 20000) {
            systemACounter++
            println("Check $systemACounter: Salary and capital income low enough for ${fiveYearIncome.personalNumber}")
            val lastOne = systemACounter == 111474

            val packageToIAV = PackageToIAV(fiveYearIncome.personalNumber, startTime, lastOne)
            sendEvent(packageToIAV)
        }
    }

    private suspend fun sendEvent(packageToIAV: PackageToIAV) {
        val client = HttpClient(CIO)

        val startTime: Long = System.currentTimeMillis()
        val response: HttpResponse = client.post("http://localhost:4570/iav/personEntitled") {
            headers {
                append("Accept", "application/json")
            }
            method = HttpMethod.Post
            contentType(ContentType.Application.Json)
            body = gson.toJson(packageToIAV)
        }
        val stopTime: Long = System.currentTimeMillis()
        client.close()

        eventLogger.logSentEvent("five_year_income_calculated_person_entitled")
    }
}
