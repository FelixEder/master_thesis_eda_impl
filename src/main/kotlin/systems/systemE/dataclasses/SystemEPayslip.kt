package systems.systemE.dataclasses

/**
 * Data class that stores information about a payslip for a specific person in system E.
 * Is used for sending information to system F.
 *
 * @author Felix Eder
 * @date 2021-05-04
 */
data class SystemEPayslip(val personalNumber: String, val year: Int, val month: Int, val income: Int)
