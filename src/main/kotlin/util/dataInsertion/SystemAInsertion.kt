package util.dataInsertion

import com.google.gson.Gson
import com.google.gson.stream.JsonReader
import systems.systemA.SystemADBManager
import systems.systemA.dataclasses.FiveYearIncome
import util.logging.CsvLogger
import java.io.InputStream
import java.io.InputStreamReader
import java.nio.file.Files
import java.nio.file.Paths

/**
 * Helper function to insert a large volume of YearlyIncome objects to the system A DB.
 * @param args Should be the path to the json-file to read from, as a string.
 *
 * @author Felix Eder
 * @date 2021-04-28
 */
fun main(args: Array<String>) {
    val gson = Gson()
    val readFromFileStringPath = args[0]

    val csvLogger = CsvLogger("src/main/kotlin/csv/systemA/database.csv",
        "src/main/kotlin/csv/systemA/incomingHttp.csv",
        "src/main/kotlin/csv/systemA/outgoingHttp.csv")

    val systemADBManager = SystemADBManager(csvLogger)

    val inputStream: InputStream = Files.newInputStream(Paths.get(readFromFileStringPath))
    val reader = JsonReader(InputStreamReader(inputStream))

    val fiveYearIncomes: MutableList<FiveYearIncome> = mutableListOf()

    var index = 0
    reader.beginArray()
    while (reader.hasNext()) {
        val yearlyIncome: FiveYearIncome = gson.fromJson(reader, FiveYearIncome::class.java)
        fiveYearIncomes.add(yearlyIncome)
        index++

        if (index >= 100000) {
            systemADBManager.batchInsertYearlyIncome(fiveYearIncomes)
            fiveYearIncomes.clear()
            index = 0
        }
    }

    systemADBManager.batchInsertYearlyIncome(fiveYearIncomes)
}