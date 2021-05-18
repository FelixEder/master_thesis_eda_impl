package systems.systemF.dataclasses

/**
 * Data class used when sending monthly income to the IAV system.
 *
 * @author Felix Eder
 * @date 2021-04-22
 */
data class IncomeReq(val personalNumber: String, val income: Int)
