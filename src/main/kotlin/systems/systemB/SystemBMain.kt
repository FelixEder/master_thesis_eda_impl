package systems.systemB

import com.google.gson.Gson
import systems.systemB.dataclasses.PostalInfo
import spark.Spark.get
import spark.Spark.port
import util.logging.CsvLogger

/**
 * A simple web service that sets up a REST api in order to get basic information about people.
 *
 * @author Felix Eder
 * @date 2021-04-13
 */
private val csvLogger = CsvLogger("src/main/kotlin/csv/systemB/database.csv",
    "src/main/kotlin/csv/systemB/incomingHttp.csv",
    "src/main/kotlin/csv/systemB/outgoingHttp.csv")
private val systemBDBManager: SystemBDBManager = SystemBDBManager(csvLogger)
private val gson = Gson()

/**
 * Main function that sets up the server
 */
fun main() {
    setUpHttpServer()
}

/**
 * Sets up a simple HTTP server that listens for GET requests and sends over the user data.
 */
private fun setUpHttpServer() {
    port(4567)
    get("/systemB") { req, res ->
        csvLogger.logIncomingHttpRequest("GET", "Incoming request to get person from systemB", req.contentLength())

        val personalNumber: String = req.queryParams("personalNumber")

        val person = systemBDBManager.getPersonByPersonalNumber(personalNumber)

        res.status(200)
        if (person == null)
            "Error, person cannot be found in systemB database."

        else
            gson.toJson(PostalInfo(person.fullName, person.homeAddress, person.postalNumber))
    }
}
