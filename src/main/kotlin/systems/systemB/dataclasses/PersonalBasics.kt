package systems.systemB.dataclasses

/**
 * Data class representing a person object used by the system B service.
 *
 * @author Felix Eder
 * @date 2021-04-12
 */
data class PersonalBasics(
    val personalNumber: String, val fullName: String, var age: Int, val dateRegistered: String, //Date format: yyyy-MM-dd
    val homeAddress: String, val postalNumber: String)
