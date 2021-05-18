package util.dataGeneration

import com.google.gson.Gson
import com.google.gson.stream.JsonReader
import com.google.gson.stream.JsonWriter
import systems.systemB.dataclasses.PersonalBasics
import systems.systemA.dataclasses.FiveYearIncome
import java.io.*
import java.nio.file.Files
import java.nio.file.Paths

private val gson = Gson()

/**
 * Generate the correct data for the system A.
 */
fun main(args: Array<String>) {
    val readFromFileStringPath = args[0]
    val writeToFileStringPath = args[1]

    val inputStream: InputStream = Files.newInputStream(Paths.get(readFromFileStringPath))
    val reader = JsonReader(InputStreamReader(inputStream))

    val outputStream: OutputStream = Files.newOutputStream(Paths.get(writeToFileStringPath))
    val writer = JsonWriter(OutputStreamWriter(outputStream))

    reader.beginArray()
    writer.beginArray()
    while (reader.hasNext()) {
        val personalBasics: PersonalBasics = gson.fromJson(reader, PersonalBasics::class.java)

        val randomSalaryIncome = (0..300000).random()
        val randomCapitalIncome = (0..60000).random()

        val yearlyIncome = FiveYearIncome(personalBasics.personalNumber, randomSalaryIncome,
            randomCapitalIncome)

        gson.toJson(yearlyIncome, FiveYearIncome::class.java, writer)
    }
    writer.endArray()
    writer.close()
}