package systems.systemA.dataclasses

/**
 * Data class with information about the salary and capital income for a specific person
 *
 * @author Felix Eder
 * @date 2021-04-06
 */
data class PersonalIncome(
    val personalNumber: String, //ID
    val yearlySalaryIncome: MutableList<Int>, //Sorted by year ascending, first value is the first yearly salary income registered for the Person.
    val yearlyCapitalIncome: MutableList<Int>, //Sorted by year ascending, first value is the first yearly capital income registered for the Person.
)