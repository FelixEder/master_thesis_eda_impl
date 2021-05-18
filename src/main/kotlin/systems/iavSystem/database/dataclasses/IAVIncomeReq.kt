package systems.iavSystem.database.dataclasses

/**
 * Data class used for modeling income responses from systemF.
 *
 * @author Felix Eder
 * @date 2021-04-22
 */
data class IAVIncomeReq(val personalNumber: String, val income: Int, val certId: Int)
