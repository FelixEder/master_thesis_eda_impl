package systems.iavSystem.stickControl

import com.google.gson.Gson
import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import systems.iavSystem.database.PSQLDBManager
import systems.iavSystem.database.dataclasses.IAVCertificate
import systems.iavSystem.stickControl.dataclasses.StickSet
import systems.systemF.dataclasses.MonthlyIncome
import util.logging.CsvLogger
import util.logging.EventLogger
import util.logging.TwoHeaderCsvLogger
import java.net.BindException

/**
 * Service that does the stick control for all valid IAV certificates, should be run monthly.
 *
 * @author Felix Eder
 * @date 2021-04-19
 */
class StickControl (private val psqlDbManager: PSQLDBManager, private val csvLogger: CsvLogger,
                    private val stickControlLogger: TwoHeaderCsvLogger, private val eventLogger: EventLogger) {
    private val gson = Gson()

    /**
     * Starts the stick control process for a single person.
     *
     * @param incomeInfo Information about the latest income for a specific person.
     */
    fun startSingleStickControl(incomeInfo: MonthlyIncome) {
        val startTime = System.currentTimeMillis()

        val cert = psqlDbManager.getCertificate(incomeInfo.personalNumber)

        if (cert == null) {
            GlobalScope.launch {
                sendEventCertificateInvalid(incomeInfo)
            }
            return
        } else {
            val stickSet = psqlDbManager.getStickSet(incomeInfo.personalNumber)
            val updatedStickSet = updateSticks(incomeInfo.personalNumber, stickSet, incomeInfo.income)
            if (controlStickSet(updatedStickSet))
                updateStickInformation(updatedStickSet)
            else {
                GlobalScope.launch {
                    unregisterCertificate(incomeInfo.personalNumber)
                }
            }
        }

        val stopTime = System.currentTimeMillis()

        stickControlLogger.log("Calculated new sticks for person: ${incomeInfo.personalNumber}", stopTime - startTime)
    }

    /**
     * Updates the stickSet for a person based on their new monthly income. If the person has no
     * stickSet yet, a new one is created and stored in the database.
     *
     * @param personalNumber The personal number of the person to update sticks for.
     * @param additionalInfo A pair with a nullable stickSet as well as this months income as an integer.
     *
     * @return The updated stickSet.
     */
    private fun updateSticks(personalNumber: String, stickSet: StickSet?, income: Int): StickSet {
        var localStickSet = stickSet
        if (stickSet == null) { //Person has no stick set, create one and store it in db.
            localStickSet = StickSet(personalNumber, 0, 0)
            psqlDbManager.insertStickSet(localStickSet)
        }

        if (income >= 5000)
            (localStickSet as StickSet).sticks5K++

        if (income >= 28000)
            (localStickSet as StickSet).sticks28K++

        return (localStickSet as StickSet)
    }

    /**
     * Controls if a given stickSet has gone over the limits of the IAV. Then returns a boolean
     * representing the result.
     *
     * @param stickSet The stickSet to control.
     * @return True if the stickSet is still under the limit and the person can keep the certificate
     * and false if vice versa.
     */
    private fun controlStickSet(stickSet: StickSet): Boolean {
        return stickSet.sticks28K < 3 && stickSet.sticks5K < 24
    }

    /**
     * Updates the stick set in the database.
     */
    private fun updateStickInformation(stickSet: StickSet) {
        psqlDbManager.updateSpecificStickSet(stickSet)
        println("Updated the stick set for: ${stickSet.personalNumber}")
    }

    /**
     * Removes the StickSet and IAVCertificate from the database for a specific person and
     * sends a request to the postalService to send out the information to the affected person.
     */
    private suspend fun unregisterCertificate(personalNumber: String) {
        psqlDbManager.removeStickSet(personalNumber)

        val iavCertificate = psqlDbManager.getCertificate(personalNumber) as IAVCertificate
        psqlDbManager.removeCertificate(iavCertificate.id)

        val client = HttpClient(CIO)

        val startTime: Long = System.currentTimeMillis()
        while (true) {
            try {
                val response: HttpResponse = client.post("http://localhost:4572/systemC/certUnregistered") {
                    headers {
                        append("Accept", "application/json")
                    }
                    method = HttpMethod.Post
                    contentType(ContentType.Application.Json)
                    body = gson.toJson(iavCertificate)
                }
                val stopTime: Long = System.currentTimeMillis()
                client.close()

                eventLogger.logSentEvent("Event sent that a person: $personalNumber is no longer eligible for an IAV-certificate")

                println("Person $iavCertificate.personalNumber has been unregistrered from IAVRegistry")
                break
            } catch (exception: BindException) {
                println("Exception when sending event that person isn't eligible anymore: $exception")
                delay(100)
                continue
            }
        }

    }

    /**
     * Sends an event that a certificate was invalid.
     *
     * @param monthlyIncome The monthly income object related to this invalid object.
     */
    private suspend fun sendEventCertificateInvalid(monthlyIncome: MonthlyIncome) {
        val client = HttpClient(CIO)

        while (true) {
            try {
                client.post<Unit>("http://localhost:4571/systemE/certificateInvalidEvent") {
                    headers {
                        append("Accept", "application/json")
                    }
                    method = HttpMethod.Post
                    contentType(ContentType.Application.Json)
                    body = gson.toJson(monthlyIncome)
                }
                client.close()

                eventLogger.logSentEvent("Event sent that certificate was invalid for person: ${monthlyIncome.personalNumber}")
                break
            } catch (exception: BindException) {
                println("Exception when sending event that certificate was invalid: $exception")
                delay(100)
                continue
            }
        }
    }
}