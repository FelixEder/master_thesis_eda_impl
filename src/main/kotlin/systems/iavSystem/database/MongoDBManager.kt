package systems.iavSystem.database

import com.mongodb.*
import systems.iavSystem.database.dataclasses.Person

/**
 * Class responsible for managing the connection to the persons collection of the IAVRegistry.
 *
 * @author Felix Eder
 * @date 2021-04-08
 */
class MongoDBManager {
    //MongoDB client stuff
    private val mongoClient: MongoClient =
        MongoClient(MongoClientURI("mongodb://localhost:27017")) //Make URI variable if it needs to be changed
    private val database: DB = mongoClient.getDB("IAVRegistry")
    private val personCollection: DBCollection =
        database.getCollection("persons") //Check that these all work

    //private val dataStore: Datastore = Morphia.createDatastore(mongoClient, "IAVRegistry")

    /**
     * Function that adds a Person object to the database.
     *
     * @param person The Person object to be added.
     */
    fun addToDatabase(person: Person) {
        personCollection.insert(convertPersonToDBObject(person))
    }

    fun updateDatabase(person: Person) {

    }

    /**
     * Get a specific Person object from the database.
     *
     * @param personalNumber The personal number of the person to get.
     * @return The Person object if found or null if nothing is found.
     */
    fun getFromDatabase(personalNumber: String): Person? {
        val query = BasicDBObject("_id", personalNumber)
        val dbCursor = personCollection.find(query)
        if (dbCursor.size() == 0) return null
        return convertDBObjectToPerson(dbCursor.one())
    }

    fun closeDatabase() {
        mongoClient.close()
    }

    /**
     * Converts a Person object into a DBObject that can be stored in the database.
     *
     * @param person The Person object to convert.
     * @return The converted DBObject.
     */
    private fun convertPersonToDBObject(person: Person): DBObject {
        return BasicDBObject("_id", person.personalNumber)
            .append("fullName", person.fullName)
            .append("age", person.age)
            .append("dateRegistered", person.dateRegistered)
            .append("yearlySalaryIncome", person.yearlySalaryIncome)
            .append("yearlyCapitalIncome", person.yearlyCapitalIncome)
            .append("homeAddress", person.homeAddress)
            .append("postalNumber", person.postalNumber)
    }

    /**
     * Converts a DBObject into a Person object that can be retrieved by the system.
     *
     * @param dbObject The DBObject to convert.
     * @return The converted Person object.
     */
    private fun convertDBObjectToPerson(dbObject: DBObject): Person {
        return Person(dbObject.get("_id") as String, dbObject.get("fullName") as String,
            dbObject.get("age") as Int,
            dbObject.get("dateRegistered") as String,
            dbObject.get("yearlySalaryIncome") as List<Int>,
            dbObject.get("yearlyCapitalIncome") as List<Int>,
            dbObject.get("homeAddress") as String,
            dbObject.get("postalNumber") as String
        )
    }
}