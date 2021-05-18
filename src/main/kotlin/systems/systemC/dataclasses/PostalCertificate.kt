package systems.systemC.dataclasses


/**
 * Data class that represents a IAV certificate to be posted for a specific person.
 *
 * @author Felix Eder
 * @date 2021-04-2
 */
data class PostalCertificate(val id: Int, val personalNumber: String, val dateIssued: String, val expirationDate: String)
