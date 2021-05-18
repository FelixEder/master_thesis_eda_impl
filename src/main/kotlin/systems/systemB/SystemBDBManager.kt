package systems.systemB

import org.jetbrains.exposed.dao.IntEntity
import org.jetbrains.exposed.dao.IntEntityClass
import org.jetbrains.exposed.dao.id.EntityID
import org.jetbrains.exposed.dao.id.IntIdTable
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction
import systems.systemB.dataclasses.PersonalBasics
import util.logging.CsvLogger

/**
 * Class that interacts with the database that systemB handles.
 *
 * @author Felix Eder
 * @date 2021-04-15
 */
class SystemBDBManager(private val csvLogger: CsvLogger) {
    private val db = Database.connect("jdbc:postgresql://localhost/PersonalBasics?rewriteBatchedInserts=true", driver = "org.postgresql.Driver",
        user = "exjobb", password = "1234")

    object Persons : IntIdTable() {
        val personalNumber: Column<String> = varchar("personalNumber", 50).uniqueIndex() //Double check that it's correct
        val fullName: Column<String> = varchar("fullName", 50)
        val age: Column<Int> = integer("age")
        val dateRegistered: Column<String> = varchar("dateRegistered", 50)
        val homeAddress: Column<String> = varchar("homeAddress", 50)
        val postalNumber: Column<String> = varchar("postalNumber", 50)
    }

    class Person(id: EntityID<Int>): IntEntity(id) {
        companion object : IntEntityClass<Person>(Persons)
        var personalNumber by Persons.personalNumber
        var fullName by Persons.fullName
        var age by Persons.age
        var dateRegistered by Persons.dateRegistered
        var homeAddress by Persons.homeAddress
        var postalNumber by Persons.postalNumber
    }

    init {
        transaction(db) {
            addLogger(StdOutSqlLogger)
            SchemaUtils.create(Persons)
        }
    }

    /**
     * Inserts a bulk of personal basics object into the database.
     *
     * @param personalBasics The list of personal basics to insert.
     */
    fun bulkInsertPerson(personalBasics: List<PersonalBasics>) {
        transaction(db) {
            addLogger(StdOutSqlLogger)

            Persons.batchInsert(personalBasics,
                ignore = false,
                shouldReturnGeneratedValues = false
            ) {
                this[Persons.personalNumber] = it.personalNumber
                this[Persons.fullName] = it.fullName
                this[Persons.age] = it.age
                this[Persons.dateRegistered] = it.dateRegistered
                this[Persons.homeAddress] = it.homeAddress
                this[Persons.postalNumber] = it.postalNumber
            }
        }
    }

    /**
     * Gets a person from the database given a personal number, or null if no person can be found for it.
     *
     * @param personalNumber The personal number to find a person for.
     * @return The person if found or null if not found.
     */
    fun getPersonByPersonalNumber(personalNumber: String): PersonalBasics? {
        var person: PersonalBasics? = null

        transaction(db) {
            addLogger(StdOutSqlLogger)

            val maybePerson = Person.find { Persons.personalNumber eq personalNumber }.firstOrNull()

            if (maybePerson != null)
                person = PersonalBasics(maybePerson.personalNumber, maybePerson.fullName, maybePerson.age,
                maybePerson.dateRegistered, maybePerson.homeAddress, maybePerson.postalNumber)
        }
        csvLogger.logDatabaseAccess("READ", "Get person from database")
        return person
    }
}