package systems.iavSystem.database.dataclasses

/**
 * Data class representing a person in the system.
 *
 * @author Felix Eder
 * @date 2021-04-06
 */
data class Person(
    val personalNumber: String, val fullName: String, var age: Int, val dateRegistered: String, //Date format: yyyy-MM-dd
    val yearlySalaryIncome: List<Int>, //Sorted by year ascending, first value is the first yearly salary income registered for the Person.
    val yearlyCapitalIncome: List<Int>, //Sorted by year ascending, first value is the first yearly capital income registered for the Person.
    val homeAddress: String,
    val postalNumber: String)
