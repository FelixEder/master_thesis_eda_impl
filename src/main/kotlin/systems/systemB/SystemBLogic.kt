package systems.systemB

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import systems.systemB.dataclasses.PersonalBasics
import java.io.BufferedReader
import java.io.File
import java.time.LocalDate

/**
 * Class that handles the underlying logic for the system B.
 *
 * @author Felix Eder
 * @date 2021-04-12
 */
class SystemBLogic {
    private val gson = Gson()
    private var personalBasics = emptyList<PersonalBasics>()

    /**
     * Starts reading the data from file and sets up a web service to send the data.
     */
    init {
        readDataFromFile()
    }

    /**
     * Reads JSON data from file and stores locally as POKO objects.
     */
    private fun readDataFromFile() {
        val bufferedReader: BufferedReader = File("src/main/kotlin/systems/systemB/resources/personsSmall.json").bufferedReader()
        val inputString = bufferedReader.use { it.readText() }

        val myType = object : TypeToken<List<PersonalBasics>>() {}.type
        personalBasics = gson.fromJson(inputString, myType)
    }

    /**
     * Goes through the stored data and returns a filtered list based on the given parameters.
     *
     * @param minAge The lowest age in the range, inclusive.
     * @param maxAge The highest age in the range, inclusive.
     * @param minAgeRegistered The needed amount of years to be registered, inclusive.
     */
    fun filterPersons(minAge: Int, maxAge: Int, minAgeRegistered: Long): List<PersonalBasics> {
        val filteredPersonalBasics: MutableList<PersonalBasics> = mutableListOf()

        for (personalBasic in personalBasics) {
            if (checkAge(minAge, maxAge, personalBasic.age)
                && checkRegistryDate(minAgeRegistered, personalBasic.dateRegistered))
                filteredPersonalBasics.add(personalBasic)
        }

        return filteredPersonalBasics
    }

    /**
     * Check if a person is between a certain age range.
     *
     * @param minAge The lowest age in the range, inclusive.
     * @param maxAge The highest age in the range, inclusive.
     * @param age The age of the person to check.
     *
     * @return True if person is in the age range and false if not.
     */
    fun checkAge(minAge: Int, maxAge: Int, age: Int): Boolean {
        return age in minAge..maxAge
    }

    /**
     * Check if a person has been registered in Sweden for a certain number of years.
     *
     * @param minAgeRegistered The needed amount of years to be registered, inclusive.
     * @param dateRegistered The date the person to check was registered in Sweden.
     *
     * @return True if a person has been registered in Sweden for at certain number of years and false
     * if vice versa.
     */
    fun checkRegistryDate(minAgeRegistered: Long, dateRegistered: String): Boolean {
        val currentDate = LocalDate.now()
        val personDate = LocalDate.parse(dateRegistered)

        return currentDate.minusYears(minAgeRegistered).isAfter(personDate)
    }
}