package systems.iavSystem.iavControl

import com.google.gson.Gson
import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import systems.systemB.SystemBDBManager
import systems.iavSystem.database.PSQLDBManager
import systems.iavSystem.database.dataclasses.IAVCertificate
import systems.iavSystem.database.dataclasses.PackageToSystemC
import systems.systemA.dataclasses.PackageToIAV
import util.logging.CsvLogger
import util.logging.EventLogger
import util.logging.TwoHeaderCsvLogger
import java.time.LocalDate


/**
 * Service that does the IAV control for a specific person.
 *
 * @author Felix Eder
 * @date 2021-04-03
 */
class IAVControl(private val psqlDbManager: PSQLDBManager, private val systemBDBManager: SystemBDBManager,
                 private val csvLogger: CsvLogger, private val iavControlLogger: TwoHeaderCsvLogger,
                 private val eventLogger: EventLogger) {
    private val gson = Gson()

    /**
     * Check if a person is eligible for a IAVCertificate.
     *
     * @param personalNumber The personal number of the person to check for.
     */
    fun makeIAVCheck(packageToIAV: PackageToIAV) {
        val person = systemBDBManager.getPersonByPersonalNumber(packageToIAV.personalNumber)

        if (person == null || checkAlreadyRegistered(packageToIAV.personalNumber)) {
            println("Person is not eligible or already has a valid certificate.")
            return
        }

        val systemCInfo = Pair(packageToIAV.lastOne, packageToIAV.startTime)

        createIAVCertificate(packageToIAV.personalNumber, systemCInfo)
    }

    /**
     * Creates an IAVCertificate and inserts it in the database.
     *
     * @param personalNumber The personal number to create the certificate for.
     */
    private fun createIAVCertificate(personalNumber: String, systemCInfo: Pair<Boolean, Long>) {
        val currentDate = LocalDate.now()
        val expirationDate = currentDate.plusYears(6)

        val certId = psqlDbManager.insertCertificate(personalNumber, currentDate.toString(), expirationDate.toString())

        val iavCertificate = IAVCertificate(certId, personalNumber, currentDate.toString(), expirationDate.toString())

        val packageToSystemC = PackageToSystemC(iavCertificate, systemCInfo.first, systemCInfo.second)

        GlobalScope.launch {
            sendCertificateToSystemC(packageToSystemC)
        }
    }

    /**
     * Sends a newly created certificate to the system C postal service in  order to send it out to the
     * person.
     *
     * @param iavCertificate The newly created IAVCertificate.
     */
    private suspend fun sendCertificateToSystemC(packageToSystemC: PackageToSystemC) {
        val client = HttpClient(CIO)

        val startTime: Long = System.currentTimeMillis()
        val response: HttpResponse = client.post("http://localhost:4572/systemC/certGranted") {
            headers {
                append("Accept", "application/json")
            }
            method = HttpMethod.Post
            contentType(ContentType.Application.Json)
            body = gson.toJson(packageToSystemC)
        }
        val stopTime: Long = System.currentTimeMillis()

        client.close()

        eventLogger.logSentEvent("Sending event that a person has been issued a IAV certificate")

        println("Person ${packageToSystemC.iavCertificate.personalNumber} has been issued a IAVCertificate")
    }

    /**
     * Check if a person already holds a valid IAV certificate and is thus in the IAV registry.
     *
     * @param personalNumber The personal number of the person to check registry for.
     * @return True if a person is already in the IAV registry and false if not.
     */
    private fun checkAlreadyRegistered(personalNumber: String): Boolean {
        return psqlDbManager.getCertificate(personalNumber) != null
    }
}