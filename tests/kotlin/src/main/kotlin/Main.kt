import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.SQLException

fun main() {
    //val url = "jdbc:postgresql://DESKTOP-KHLB071:5432/test"
    val url = "jdbc:postgresql://localhost:5432/scada" // Adjust the URL as needed
    val user = "username1"
    val password = "password1"

    //val url = "jdbc:postgresql://scada:5432/scada"
    //val user = "system"
    //val password = "manager"

    val queries = java.io.File("queries.txt")
            .readText()
            .splitToSequence(";")
            .map { it.trim() }
            .filter { it.isNotEmpty() }
            .map { "$it;" }
            .toList()

    try {
        val props = java.util.Properties().apply {
                    setProperty("user", user)
                    setProperty("password", password)
                    setProperty("binaryTransfer", "true")
                }
        DriverManager.getConnection(url, props).use { connection ->
            println("Successfully connected to PostgreSQL database!")

            connection.createStatement().use { statement ->
                for ((idx, query) in queries.withIndex()) {
                    println("\nExecuting Query ${idx + 1}: $query")
                    statement.executeQuery(query).use { resultSet ->
                        println("\n--- Query Results ---")
                        printResults(resultSet)
                        println("--- End of Results ---")
                    }
                }
            }
        }
        println("\nConnection closed.")
    } catch (e: SQLException) {
        println("Database connection or query failed: ${e.message}")
        e.printStackTrace()
    }
}

/**
 * Helper function to print the contents of a ResultSet, row by row.
 */
fun printResults(rs: ResultSet) {
    val metaData = rs.metaData
    val columnCount = metaData.columnCount

    // Collect all rows into a list of lists
    val rows = mutableListOf<List<String>>()
    while (rs.next()) {
        val row = (1..columnCount).map { rs.getString(it) ?: "NULL" }
        rows.add(row)
    }

    if (rows.isEmpty()) {
        println("No rows found matching the criteria.")
        return
    }

    println("Total rows: ${rows.size}")

    // Determine maximum column widths
    val columnWidths = IntArray(columnCount) { 0 }

    // Header names width
    for (i in 1..columnCount) {
        val headerName = metaData.getColumnName(i)
        columnWidths[i - 1] = maxOf(columnWidths[i - 1], headerName.length)
    }

    // Data types width (for the first row)
    for (i in 1..columnCount) {
        val dataType = metaData.getColumnTypeName(i)
        columnWidths[i - 1] = maxOf(columnWidths[i - 1], dataType.length)
    }

    // Row data width
    for (row in rows) {
        for ((colIdx, cell) in row.withIndex()) {
            columnWidths[colIdx] = maxOf(columnWidths[colIdx], cell.length)
        }
    }

    // Print column headers
    val headerNames = (1..columnCount).joinToString(" | ") { colIdx ->
        metaData.getColumnName(colIdx).padEnd(columnWidths[colIdx - 1])
    }
    println(headerNames)

    // Print data types for the first row
    val dataTypes = (1..columnCount).joinToString(" | ") { colIdx ->
        metaData.getColumnTypeName(colIdx).padEnd(columnWidths[colIdx - 1])
    }
    println(dataTypes)

    // Print separator
    println(columnWidths.joinToString("-+-") { "-".repeat(it) })

    // Print rows
    for (row in rows.take(10)) { // Print only the first 10 rows
        val formattedRow = row.mapIndexed { colIdx, cell ->
            cell.padEnd(columnWidths[colIdx])
        }.joinToString(" | ")
        println(formattedRow)
    }
}