package util.dataInsertion

import com.google.gson.Gson
import com.google.gson.stream.JsonReader
import systems.systemB.SystemBDBManager
import systems.systemB.dataclasses.PersonalBasics
import util.logging.CsvLogger
import java.io.InputStream
import java.io.InputStreamReader
import java.nio.file.Files
import java.nio.file.Paths

/**
 * Helper function to insert a large volume of PersonalBasics objects to the system B DB
 * @param args Should be the path to the json-file to read from, as a string.
 *
 * @author Felix Eder
 * @date 2021-04-28
 */
fun main(args: Array<String>) {
    val gson = Gson()
    val readFromFileStringPath = args[0]

    val csvLogger = CsvLogger("src/main/kotlin/csv/systemB/database.csv",
        "src/main/kotlin/csv/systemB/incomingHttp.csv",
        "src/main/kotlin/csv/systemB/outgoingHttp.csv")
    val SystemBDBManager = SystemBDBManager(csvLogger)

    val inputStream: InputStream = Files.newInputStream(Paths.get(readFromFileStringPath))
    val reader = JsonReader(InputStreamReader(inputStream))

    val manyPersonalBasics: MutableList<PersonalBasics> = mutableListOf()

    var index = 0

    reader.beginArray()

    while (reader.hasNext()) {
        val personalBasics: PersonalBasics = gson.fromJson(reader, PersonalBasics::class.java)
        manyPersonalBasics.add(personalBasics)
        index++

        if (index >= 100000) {
            SystemBDBManager.bulkInsertPerson(manyPersonalBasics)
            manyPersonalBasics.clear()
            index = 0
        }
    }

    SystemBDBManager.bulkInsertPerson(manyPersonalBasics)
}