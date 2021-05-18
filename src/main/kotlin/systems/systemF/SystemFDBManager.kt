package systems.systemF

import kotlinx.coroutines.Dispatchers
import org.jetbrains.exposed.dao.IntEntity
import org.jetbrains.exposed.dao.IntEntityClass
import org.jetbrains.exposed.dao.id.EntityID
import org.jetbrains.exposed.dao.id.IntIdTable
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.experimental.suspendedTransactionAsync
import org.jetbrains.exposed.sql.transactions.transaction
import util.logging.CsvLogger

/**
 * Database manager for the system F that sets up and interacts with the database.
 *
 * @author Felix Eder
 * @date 2021-04-17
 */
class SystemFDBManager(val csvLogger: CsvLogger) {
    private val db = Database.connect("jdbc:postgresql://localhost/systemF?reWriteBatchedInserts=true", driver = "org.postgresql.Driver",
        user = "exjobb", password = "1234")

    object MonthlyIncomes : IntIdTable() {
        val personalNumber: Column<String> = varchar("personalNumber", 50)
        val year: Column<Int> = integer("year")
        val month: Column<Int> = integer("month")
        val income: Column<Int> = integer("income")
    }

    class MonthlyIncome(id: EntityID<Int>): IntEntity(id) {
        companion object : IntEntityClass<MonthlyIncome>(MonthlyIncomes)
        var personalNumber by MonthlyIncomes.personalNumber
        var year by MonthlyIncomes.year
        var month by MonthlyIncomes.month
        var income by MonthlyIncomes.income
    }

    init {
        transaction(db) {
            addLogger(StdOutSqlLogger)
            SchemaUtils.create(MonthlyIncomes)
        }
    }

    /**
     * Store a monthly income for a person.
     *
     * @param personalNumber The personal number of the person to store the income for.
     * @param year The year to store the income as.
     * @param income The income the person had for the year.
     */
    fun insertMonthlyIncome(personalNumber: String, year: Int, month: Int, income: Int) {
        transaction(db) {
            addLogger(StdOutSqlLogger)

            MonthlyIncome.new {
                this.personalNumber = personalNumber
                this.year = year
                this.month = month
                this.income = income
            }
        }
        csvLogger.logDatabaseAccess("WRITE", "Insert income into database")
    }

    /**
     * Bulk inserts many monthly incomes into the db.
     *
     * @param incomes The list of incomes to insert.
     */
    suspend fun bulkInsertMonthlyIncome(incomes: List<systems.systemF.dataclasses.MonthlyIncome>) {
        val startTime = System.currentTimeMillis()
        val result = suspendedTransactionAsync(Dispatchers.IO, db) {
            addLogger(StdOutSqlLogger)

            MonthlyIncomes.batchInsert(incomes,
                false,
                shouldReturnGeneratedValues = false) {
                this[MonthlyIncomes.personalNumber] = it.personalNumber
                this[MonthlyIncomes.year] = it.year
                this[MonthlyIncomes.month] = it.month
                this[MonthlyIncomes.income] = it.income
            }
        }
        result.await()

        val stopTime = System.currentTimeMillis()
        csvLogger.logDatabaseAccess("WRITE", "Bulk insert monthly incomes into database", startTime, stopTime)
    }

    /**
     * Gets a monthly income for a specific person, year and month.
     *
     * @param personalNumber The personal number of the person to get the income for.
     * @param year The year to get the personal information for.
     * @param month The month to get the income for.
     * @return The income or 0 if not found.
     */
    fun getMonthlyIncome(personalNumber: String, year: Int, month: Int): Int {
        var income = 0

        val startTime = System.currentTimeMillis()
        transaction(db) {
            addLogger(StdOutSqlLogger)

            val result = MonthlyIncome.find {
                MonthlyIncomes.personalNumber eq personalNumber and (MonthlyIncomes.year eq year) and (MonthlyIncomes.month eq month)
            }.firstOrNull()

            if (result != null) income = result.income
        }
        val stopTime = System.currentTimeMillis()
        csvLogger.logDatabaseAccess("READ", "Get specific income from database", startTime, stopTime)

        return income
    }
}