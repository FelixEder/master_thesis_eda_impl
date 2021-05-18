package util.dataGeneration

import com.google.gson.Gson
import com.google.gson.stream.JsonReader
import com.google.gson.stream.JsonWriter
import systems.systemB.dataclasses.PersonalBasics
import systems.systemD.dataclasses.Employee
import systems.iavSystem.database.PSQLDBManager
import util.logging.CsvLogger
import java.io.InputStream
import java.io.InputStreamReader
import java.io.OutputStream
import java.io.OutputStreamWriter
import java.nio.file.Files
import java.nio.file.Paths

private val gson = Gson()

/**
 * File for generating
 */
fun main(args: Array<String>) {
    val numberOfEmployees: Int = args[0].toInt()
    val numberOfEmployers: Int = args[1].toInt()
    val iavPercentageDigit: Int = args[2].toInt()

    val iavCsvLogger = CsvLogger("src/main/kotlin/csv/iavSystem/database.csv",
        "src/main/kotlin/csv/iavSystem/incomingHttp.csv",
        "src/main/kotlin/csv/iavSystem/outgoingHttp.csv")

    val psqldbManager = PSQLDBManager(iavCsvLogger)

    val systemBInputStream: InputStream = Files.newInputStream(Paths.get("src/main/kotlin/services/systemB/resources/systemBDataLarge.json"))
    val systemBReader = JsonReader(InputStreamReader(systemBInputStream))

    systemBReader.beginArray()
    for (i in 1 .. numberOfEmployers) {
        val outputStream: OutputStream = Files.newOutputStream(Paths.get("src/main/kotlin/services/employer/resources/employeeData$i.json"))
        val writer = JsonWriter(OutputStreamWriter(outputStream))
        writer.beginArray()

        val numberOfIav = numberOfEmployees / iavPercentageDigit

        val offset: Long = (i * numberOfIav).toLong()

        val certs = psqldbManager.getCerts(offset, numberOfIav)

        for (cert in certs) {
            val employee = Employee(cert.personalNumber, true, cert.id)
            gson.toJson(employee, Employee::class.java, writer)
        }

        for (j in 0 until numberOfEmployees - numberOfIav) {
            val personalBasics: PersonalBasics = gson.fromJson(systemBReader, PersonalBasics::class.java)
            val random = (0..10).random() //Simulate mistake by making some people have iav
            val employee = Employee(personalBasics.personalNumber, random < 3, -1)
            gson.toJson(employee, Employee::class.java, writer)
        }

        writer.endArray()
        writer.close()
    }
}