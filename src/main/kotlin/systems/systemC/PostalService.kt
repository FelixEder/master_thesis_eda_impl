package systems.systemC

import com.google.gson.JsonObject
import com.google.gson.JsonParser
import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import systems.iavSystem.database.dataclasses.PackageToSystemC
import systems.systemC.dataclasses.PostalCertificate
import util.logging.CsvLogger
import util.logging.TwoHeaderCsvLogger
import java.io.File

/**
 * Class responsible for posting out data to workers and employers.
 *
 * @author Felix Eder
 * @date 2021-04-03
 */
class PostalService(private val csvLogger: CsvLogger, private val systemCCounter: TwoHeaderCsvLogger) {

    /**
     * Starts a new certificate assignment for a specific person.
     *
     * @param iavCertificate The certificate to send out to a specific person.
     */
    suspend fun startCertificateAssignment(packageToSystemC: PackageToSystemC) {
        val personalInfo = getPersonalData(packageToSystemC.iavCertificate.personalNumber)

        sendOutCertificateGranted(packageToSystemC, personalInfo)
    }

    /**
     * Start sending out information to a person that their certificate has been unregistered.
     *
     * @param iavCertificate The unregistered IAVCertificate.
     */
    suspend fun startCertificateUnregistered(iavCertificate: PostalCertificate) {
        val personalInfo = getPersonalData(iavCertificate.personalNumber)

        sendOutCertificateUnregistered(iavCertificate, personalInfo)
    }

    /**
     * Gets supplemental personal information from the system B through Http.
     *
     * @param personalNumber The personal number of the person to get more information for.
     * @return A pair with information about a person's name and home address.
     */
    private suspend fun getPersonalData(personalNumber: String): Triple<String, String, String> {
        val client = HttpClient(CIO)

        val startTime = System.currentTimeMillis()
        val response: HttpResponse = client.request("http://localhost:4567/systemB?personalNumber=$personalNumber") {
            headers {
                append("Accept", "application/json")
            }
            method = HttpMethod.Get
        }
        val stopTime = System.currentTimeMillis()

        client.close()

        csvLogger.logOutgoingHttpRequest("GET","Logging outgoing GET request to system B from system C", response.contentLength(), startTime, stopTime)

        val jsonObject: JsonObject = JsonParser.parseString(response.readText()).asJsonObject
        return Triple(jsonObject.get("fullName").asString, jsonObject.get("homeAddress").asString,
                        jsonObject.get("postalNumber").asString)
    }

    /**
     * "Sends" out the certificate to the person by printing out the information to the terminal.
     *
     * @param iavCertificate The certificate to send out.
     * @param personalInfo A triple of personal information, in the form of <fullName, homeAddress, postalNUmber>
     */
    private fun sendOutCertificateGranted(packageToSystemC: PackageToSystemC, personalInfo: Triple<String, String, String>) {
        val stringBuilder: StringBuilder = StringBuilder()

        stringBuilder.append("Skickas till: ${personalInfo.second} ${personalInfo.third}\n")
        stringBuilder.append("Personnummer: ${packageToSystemC.iavCertificate.personalNumber}\n")

        stringBuilder.append("Grattis ${personalInfo.first}, du har blivit tilldelad ett ingångsavdrag med id: ${packageToSystemC.iavCertificate.id}\n")
        stringBuilder.append("Det är giltigt från ${packageToSystemC.iavCertificate.dateIssued} fram till och med ${packageToSystemC.iavCertificate.expirationDate}\n")

        println(stringBuilder.toString())

        val stopTime = System.currentTimeMillis()

        systemCCounter.log("${packageToSystemC.iavCertificate.personalNumber} has gotten certificate", stopTime - packageToSystemC.startTime)

        if (packageToSystemC.lastCert) {
            File("src/main/kotlin/csv/systemC/summaryScenario1.txt").writeText(
                "Finished sending out last certificate at time: $stopTime"
            )
            println("$stopTime for safety")
        }
    }

    /**
     * "Sends" out information to a person that their certificate has been unregistered
     *  by printing out the information to the terminal.
     *
     * @param iavCertificate The certificate to send out.
     * @param personalInfo A triple of personal information, in the form of <fullName, homeAddress, postalNUmber>
     */
    private fun sendOutCertificateUnregistered(iavCertificate: PostalCertificate, personalInfo: Triple<String, String, String>) {
        val stringBuilder: StringBuilder = StringBuilder()

        stringBuilder.append("Skickas till: ${personalInfo.second} ${personalInfo.third}\n")
        stringBuilder.append("Personnummer: ${iavCertificate.personalNumber}\n")

        stringBuilder.append("${personalInfo.first}, ditt ingångsavdrag med id: ${iavCertificate.id} har tyvärr fallit ur registret då din personliga inkomst har överstigit gränsen.\n")

        println(stringBuilder.toString())
    }
}