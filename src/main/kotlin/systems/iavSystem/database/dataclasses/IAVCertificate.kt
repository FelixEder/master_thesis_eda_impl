package systems.iavSystem.database.dataclasses

/**
 * Data class that represents a IAV certificate for a specific person.
 *
 * @author Felix Eder
 * @date 2021-04-15
 */
data class IAVCertificate(val id: Int, val personalNumber: String, val dateIssued: String, val expirationDate: String)
