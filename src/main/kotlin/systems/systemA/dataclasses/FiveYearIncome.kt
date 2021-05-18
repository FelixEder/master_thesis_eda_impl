package systems.systemA.dataclasses

/**
 * The income sum for salary and capital over the last five years for a person.
 *
 * @author Felix Eder
 * @date 2021-04-29
 */
data class FiveYearIncome(val personalNumber: String, val salaryIncome: Int, val capitalIncome: Int)
