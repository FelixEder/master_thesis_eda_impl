package systems.iavSystem.database

import org.jetbrains.exposed.dao.IntEntity
import org.jetbrains.exposed.dao.IntEntityClass
import org.jetbrains.exposed.dao.id.EntityID
import org.jetbrains.exposed.dao.id.IntIdTable
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction
import systems.iavSystem.database.dataclasses.IAVCertificate
import systems.iavSystem.stickControl.dataclasses.StickSet
import util.logging.CsvLogger

/**
 * Class that manages the direct interaction with the IAV PostgresSQL database.
 *
 * @author Felix Eder
 * @date 2021-04-14
 */
class PSQLDBManager(private val csvLogger: CsvLogger) {
    private val db = Database.connect("jdbc:postgresql://localhost/iavRegistry", driver = "org.postgresql.Driver",
        user = "exjobb", password = "1234")

    //Database relations
    object EdaCerts : IntIdTable() {
        val personalNumber: Column<String> = varchar("personalNumber", 50).uniqueIndex()
        val dateIssued: Column<String> = varchar("dateIssued", 50)
        val expirationDate: Column<String> = varchar("expirationDate", 50)
    }

    object DBStickSets : IntIdTable() {
        val personalNumber: Column<String> = varchar("personalNumber", 50).uniqueIndex()
        val sticks28K: Column<Int> = integer("sticks28K")
        val sticks5K: Column<Int> = integer("sticks5K")
    }

    //Database classes
    class EdaCert(id: EntityID<Int>): IntEntity(id) {
        companion object : IntEntityClass<EdaCert>(EdaCerts)
        var personalNumber by EdaCerts.personalNumber
        var dateIssued by EdaCerts.dateIssued
        var expirationDate by EdaCerts.expirationDate
    }

    class DBStickSet(id: EntityID<Int>): IntEntity(id) {
        companion object : IntEntityClass<DBStickSet>(DBStickSets)
        var personalNumber by DBStickSets.personalNumber
        var sticks28K by DBStickSets.sticks28K
        var sticks5K by DBStickSets.sticks5K
    }

    init {
        transaction(db) {
            addLogger(StdOutSqlLogger)
            SchemaUtils.create(EdaCerts, DBStickSets)
        }
    }

    //Certificate functions

    /**
     * Inserts a single IAVCertificate in the database.
     *
     * @param personalNumber The personal number of the person to check the
     * @param dateIssued
     * @param expirationDate
     * @return The id of the newly created certificate.
     */
    fun insertCertificate(personalNumber: String, dateIssued: String, expirationDate: String): Int {
        var id = 0

        val startTime = System.currentTimeMillis()
        transaction(db) {
            addLogger(StdOutSqlLogger)

            val cert: EdaCert = EdaCert.new {
                this.personalNumber = personalNumber
                this.dateIssued = dateIssued
                this.expirationDate = expirationDate
            }
            id = cert.id.value
        }
        val stopTime = System.currentTimeMillis()
        csvLogger.logDatabaseAccess("WRITE", "Insert certificate in database", startTime, stopTime)
        return id
    }

    /**
     * Gets a single IAVCertificate for a specific personalNumber, or null if not found.
     *
     * @param personalNumber The personal number of the person to get an IAVCertificate for.
     * @return An IAVCertificate if one exists, or null if not found.
     */
    fun getCertificate(personalNumber: String): IAVCertificate? {
        var iavCertificate: IAVCertificate? = null

        transaction(db) {
            addLogger(StdOutSqlLogger)
            val iavCert =  EdaCert.find { EdaCerts.personalNumber eq personalNumber }.firstOrNull()

            if (iavCert != null)
                iavCertificate = IAVCertificate(iavCert.id.value, iavCert.personalNumber, iavCert.dateIssued, iavCert.expirationDate)
        }

        csvLogger.logDatabaseAccess("READ","Get certificate from database by personal number")
        return iavCertificate
    }

    /**
     * Gets a single IAVCertificate on a given certificate id, or null if not found.
     *
     * @param certId The id of the certificate to find.
     * @return An IAVCertificate if one exists, or null if not found.
     */
    fun getCertificate(certId: Int): IAVCertificate? {
        var iavCertificate: IAVCertificate? = null

        transaction(db) {
            addLogger(StdOutSqlLogger)
            val iavCert = EdaCert.findById(certId)

            if (iavCert != null)
                iavCertificate = IAVCertificate(iavCert.id.value, iavCert.personalNumber, iavCert.dateIssued, iavCert.expirationDate)
        }
        csvLogger.logDatabaseAccess("READ","Get certificate from database by certificate id")
        return iavCertificate
    }

    /**
     * Checks if a certificate exists for a specific user in the database.
     *
     * @param personalNumber The personal number of the person to check for.
     * @param certificateId The id of the certificate to check for.
     * @return True if the certificate does exist for the person and false if not.
     */
    fun doesCertificateExist(personalNumber: String, certificateId: Int): Boolean {
        var exists = false

        transaction(db) {
            addLogger(StdOutSqlLogger)
            val cert = EdaCert.findById(certificateId)

            exists = (cert != null && cert.personalNumber == personalNumber)
        }

        csvLogger.logDatabaseAccess("READ", "Check if certificate exists in database")
        return exists
    }

    /**
     * Removes a certificate from the database based on the given id.
     *
     * @param certificateId The id of the certificate to remove.
     */
    fun removeCertificate(certificateId: Int) {
        transaction(db) {
            addLogger(StdOutSqlLogger)
            EdaCert.findById(certificateId)?.delete()
        }

        csvLogger.logDatabaseAccess("WRITE", "Remove certificate from database")
    }

    /**
     * Gets a number of IAVCertificates based on the given parameters.
     *
     * @param offset The offset to get the certificates from.
     * @param limit The amount of certificates to get.
     * @return The list of certificates found.
     */
    fun getCerts(offset: Long, limit: Int): MutableList<IAVCertificate> {
        val certs = mutableListOf<IAVCertificate>()
        transaction(db) {
            addLogger(StdOutSqlLogger)

            EdaCert.all().limit(limit, offset)
                .forEach { certs.add(
                    IAVCertificate(it.id.value, it.personalNumber,
                    it.dateIssued, it.expirationDate)
                )}
        }
        return certs
    }

    /**
     * Gets all the certificate ids from the database.
     *
     * @return The list of ids.
     */
    fun getAllCertIds(): List<Int> {
        val certIds = mutableListOf<Int>()

        transaction(db) {
            EdaCert.all().forEach { certIds.add(it.id.value) }
        }
        csvLogger.logDatabaseAccess("READ", "Get all certificate ids")

        return certIds
    }

    //Stick set functions

    /**
     * Inserts a new stickSet object in the database.
     *
     * @param stickSet The stickSet object to insert into the database.
     */
    fun insertStickSet(stickSet: StickSet) {
        val startTime = System.currentTimeMillis()
        transaction(db) {
            addLogger(StdOutSqlLogger)

            DBStickSet.new {
                personalNumber = stickSet.personalNumber
                sticks28K = stickSet.sticks28K
                sticks5K = stickSet.sticks5K
            }
        }

        val stopTime = System.currentTimeMillis()
        csvLogger.logDatabaseAccess("WRITE", "Insert stick set to database", startTime, stopTime)
    }

    /**
     * Removes a specific stick set for a person.
     *
     * @param personalNumber The personal number of the stick set to remove.
     */
    fun removeStickSet(personalNumber: String) {
        val startTime = System.currentTimeMillis()
        transaction(db) {
            addLogger()

            val dbStickSet = DBStickSet.find { DBStickSets.personalNumber eq personalNumber }.firstOrNull()
            dbStickSet?.delete()
        }
        val stopTime = System.currentTimeMillis()
        csvLogger.logDatabaseAccess("WRITE", "Remove stick set in database", startTime, stopTime)
    }

    /**
     * Gets a single StickSet for a specific person, or null if not found.
     *
     * @param personalNumber The personal number of the person to get the StickSet for.
     * @return The StickSet if found, or null if not found.
     */
    fun getStickSet(personalNumber: String): StickSet? {
        var stickSet: StickSet? = null

        val startTime = System.currentTimeMillis()
        transaction(db) {
            addLogger(StdOutSqlLogger)

            val dbStickSet = DBStickSet.find { DBStickSets.personalNumber eq personalNumber }.firstOrNull()

            if (dbStickSet != null)
                stickSet = StickSet(dbStickSet.personalNumber, dbStickSet.sticks28K, dbStickSet.sticks5K)
        }

        val stopTime = System.currentTimeMillis()
        csvLogger.logDatabaseAccess("READ", "Get stick set from database", startTime, stopTime)

        return stickSet
    }

    /**
     * Updates a stick set in the database with the given stickset.
     *
     * @param stickSet The new stickSet to update it to.
     */
    fun updateSpecificStickSet(stickSet: StickSet) {
        val startTime = System.currentTimeMillis()

        transaction(db) {
            addLogger()

            val dbStickSet = DBStickSet.find { DBStickSets.personalNumber eq stickSet.personalNumber }.firstOrNull()
            dbStickSet?.sticks28K = stickSet.sticks28K
            dbStickSet?.sticks5K = stickSet.sticks5K
        }

        val stopTime = System.currentTimeMillis()
        csvLogger.logDatabaseAccess("WRITE", "Update stick set in database", startTime, stopTime)
    }
}