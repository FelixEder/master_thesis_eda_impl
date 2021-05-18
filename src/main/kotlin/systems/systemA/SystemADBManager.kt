package systems.systemA

import org.jetbrains.exposed.dao.IntEntity
import org.jetbrains.exposed.dao.IntEntityClass
import org.jetbrains.exposed.dao.id.EntityID
import org.jetbrains.exposed.dao.id.IntIdTable
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction
import systems.systemA.dataclasses.FiveYearIncome
import util.logging.CsvLogger

/**
 * Class that sets up and manages the database associated with the system A.
 *
 * @author Felix Eder
 * @date 2021-04-15
 */
class SystemADBManager(private val csvLogger: CsvLogger) {
    private val db = Database.connect("jdbc:postgresql://localhost/PersonalIncome?rewriteBatchedInserts=true", driver = "org.postgresql.Driver",
        user = "exjobb", password = "1234")

    object FiveYearPersonalIncomes : IntIdTable() {
        val personalNumber: Column<String> = varchar("personalNumber", 50).uniqueIndex()
        val salaryIncome: Column<Int> = integer("salaryIncome")
        val capitalIncome: Column<Int> = integer("capitalIncome")
    }

    class FiveYearPersonalIncome(id: EntityID<Int>): IntEntity(id) {
        companion object : IntEntityClass<FiveYearPersonalIncome>(FiveYearPersonalIncomes)
        var personalNumber by FiveYearPersonalIncomes.personalNumber
        var salaryIncome by FiveYearPersonalIncomes.salaryIncome
        var capitalIncome by FiveYearPersonalIncomes.capitalIncome
    }

    init {
        transaction(db) {
            addLogger(StdOutSqlLogger)
            SchemaUtils.create(FiveYearPersonalIncomes)
        }
    }

    /**
     * Inserts a batch of yearly income objects into the database.
     *
     * @param fiveYearIncomes The list of yearly incomes to insert.
     */
    fun batchInsertYearlyIncome(fiveYearIncomes: List<FiveYearIncome>) {
        transaction(db) {
            addLogger(StdOutSqlLogger)

            FiveYearPersonalIncomes.batchInsert(fiveYearIncomes,
                ignore = false,
                shouldReturnGeneratedValues = false
            ) {
                this[FiveYearPersonalIncomes.personalNumber] = it.personalNumber
                this[FiveYearPersonalIncomes.salaryIncome] = it.salaryIncome
                this[FiveYearPersonalIncomes.capitalIncome] = it.capitalIncome
            }
        }
    }

    /**
     * Gets all the income ids from the database.
     *
     * @return The list of ids.
     */
    fun getAllIncomeIds(): List<Int> {
        val incomeIds = mutableListOf<Int>()

        transaction(db) {
            FiveYearPersonalIncome.all().forEach { incomeIds.add(it.id.value) }
        }
        //Since this is a "hidden" database read that is not part of the simulation, it will not be logged
        return incomeIds
    }

    /**
     * Gets all the incomes from the database.
     *
     * @return The list of incomes.
     */
    fun getAllIncomes(): List<FiveYearIncome> {
        val incomes = mutableListOf<FiveYearIncome>()

        transaction(db) {
            addLogger(StdOutSqlLogger)

            FiveYearPersonalIncome.all().forEach { incomes.add(FiveYearIncome(it.personalNumber, it.salaryIncome, it.capitalIncome)) }
        }

        return incomes
    }

    /**
     * Gets all the persons from the database that fill a specific mold.
     * @param maxSalary The maximum salary amount for the person.
     * @param maxCapital The maximum capital amount for the person.
     *
     * @return a list of all the specified persons.
     */
    fun getAllValidPersons(maxSalary: Int, maxCapital: Int): MutableList<FiveYearIncome> {
        val personalNumbers = mutableListOf<FiveYearIncome>()
        transaction(db) {
            addLogger(StdOutSqlLogger)
            FiveYearPersonalIncome.find { FiveYearPersonalIncomes.salaryIncome lessEq  maxSalary and(FiveYearPersonalIncomes.capitalIncome lessEq maxCapital) }
                .forEach { personalNumbers.add(FiveYearIncome(it.personalNumber, it.salaryIncome, it.capitalIncome)) }
        }
        csvLogger.logDatabaseAccess("READ", "Get personal numbers from database")

        return personalNumbers
    }

    /**
     * Gets a single income record for a specific id, or null if not found.
     *
     * @param incomeId The id of the income record to find.
     * @return An income record if one exists, or null if not found.
     */
    fun getIncomeById(incomeId: Int): FiveYearIncome? {
        var income: FiveYearIncome? = null

        val startTime = System.currentTimeMillis()
        transaction(db) {
            addLogger(StdOutSqlLogger)
            val dbIncome = FiveYearPersonalIncome.findById(incomeId)

            if (dbIncome != null)
                income = FiveYearIncome(dbIncome.personalNumber, dbIncome.salaryIncome, dbIncome.capitalIncome)
        }
        val stopTime = System.currentTimeMillis()
        csvLogger.logDatabaseAccess("READ", "Get specific income record for an id", startTime, stopTime)
        return income
    }
}