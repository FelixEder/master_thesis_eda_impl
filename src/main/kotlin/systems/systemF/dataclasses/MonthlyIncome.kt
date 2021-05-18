package systems.systemF.dataclasses

/**
 * Data class representing a monthly income for a person.
 *
 * @author Felix Eder
 * @date 2021-04-17
 */
data class MonthlyIncome(val employerId: Int, val personalNumber: String, val hasCert: Boolean, val certId: Int,
                         val year: Int, val month: Int, val income: Int)

