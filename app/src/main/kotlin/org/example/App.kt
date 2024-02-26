package org.example

import kotlin.system.measureTimeMillis
import java.sql.Connection
import java.sql.DriverManager

fun main() {
    DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/test", "root", "root").use { connection ->
        connection.createStatement().use { statement ->
            statement.execute("set foreign_key_checks = 0")
            (1..50).forEach {
                statement.execute("drop table test_$it")
            }
            statement.execute("set foreign_key_checks = 1")

            statement.execute(
                """
                    create table test_1 (
                        id int unsigned not null auto_increment,
                        primary key (id)
                    )
                """.trimIndent()
            )

            (2..50).forEach {
                statement.execute(
                    """
                        create table test_$it (
                            id int unsigned not null auto_increment,
                            primary_id int unsigned not null,
                            primary key (id),
                            foreign key fk_primary_id (primary_id) references test_${it - 1} (id)
                        )
                    """.trimIndent()
                )
            }
        }

        val truncateAveTime = (1..10).map {
            insertData(connection)

            measureTimeMillis {
                connection.createStatement().apply {
                    addBatch("set foreign_key_checks = 0")
                    (1..50).forEach {
                        addBatch("truncate test_$it")
                    }
                    addBatch("set foreign_key_checks = 1")
                }.executeBatch()
            }.also { println(it) }
        }.average()

        connection.createStatement().execute("set foreign_key_checks = 0")
        (1..50).forEach {
            connection.createStatement().execute("truncate test_$it")
        }
        connection.createStatement().execute("set foreign_key_checks = 1")
        val deleteAveTime = (1..10).map {
            insertData(connection, (it - 1) * 10)

            measureTimeMillis {
                connection.createStatement().apply {
                    addBatch("set foreign_key_checks = 0")
                    (1..50).forEach { addBatch("delete from test_$it") }
                    addBatch("set foreign_key_checks = 1")
                }.executeBatch()
            }.also { println(it) }
        }.average()

        println(truncateAveTime)
        println(deleteAveTime)
    }
}

fun insertData(connection: Connection, offset: Int = 0) {
    connection.createStatement().use { statement ->
        (1..10).forEach { _ ->
            statement.execute("insert into test_1 values ()")
        }

        (2..50).forEach { tableNum ->
            (1..10).forEach {
                statement.execute("insert into test_$tableNum (primary_id) values (${it + offset})")
            }
        }
    }
}
