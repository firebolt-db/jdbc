package integration.tests;

import com.firebolt.jdbc.testutils.TestTag;
import integration.IntegrationTest;
import lombok.CustomLog;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@CustomLog
@Tag(TestTag.V2)
class TransactionTest extends IntegrationTest {

	@BeforeAll
	void beforeAll() {
		executeStatementFromFile("/statements/transaction/ddl.sql");
	}

	@AfterAll
	void afterAll() {
		executeStatementFromFile("/statements/transaction/cleanup.sql");
	}

	@Test
	void shouldRemoveTransactionIdOnCommit() throws SQLException {
		try (Connection connection = createConnection();
			 Statement statement = connection.createStatement()) {
			statement.execute("BEGIN TRANSACTION;");
			statement.execute("INSERT INTO transaction_test VALUES (1, 'test')");

			checkRecordCountByIdInAnotherTransaction(1, 0, "Data should not be visible before commit");

			Properties fireboltProperties = connection.getClientInfo();
			assertNotNull(fireboltProperties.get("transaction_id"));
			statement.execute("COMMIT;");

			checkRecordCountByIdInAnotherTransaction(1, 1, "Data should be visible after commit");

			fireboltProperties = connection.getClientInfo();
			assertNull(fireboltProperties.get("transaction_id"));
		}
	}

	@Test
	void shouldRemoveTransactionIdOnRollback() throws SQLException {
		try (Connection connection = createConnection();
			 Statement statement = connection.createStatement()) {
			statement.execute("BEGIN TRANSACTION;");
			statement.execute("INSERT INTO transaction_test VALUES (2, 'test')");

			checkRecordCountByIdInAnotherTransaction(2, 0, "Data should not be visible before commit");
			Properties fireboltProperties = connection.getClientInfo();
			assertNotNull(fireboltProperties.get("transaction_id"));
			statement.execute("ROLLBACK;");

			checkRecordCountByIdInAnotherTransaction(2, 0, "Data should not be visible after rollback");

			fireboltProperties = connection.getClientInfo();
			assertNull(fireboltProperties.get("transaction_id"));
		}
	}

	@Test
	void shouldCommitTransactionWhenSwitchingToAutoCommit() throws SQLException {
		try (Connection connection = createConnection();
			 Statement statement = connection.createStatement()) {
			
			connection.setAutoCommit(false);

			statement.execute("INSERT INTO transaction_test VALUES (3, 'test')");

			checkRecordCountByIdInAnotherTransaction(3, 0, "Data should not be visible before commit");

			Properties properties = connection.getClientInfo();
			assertNotNull(properties.get("transaction_id"));
			
			connection.setAutoCommit(true);

			checkRecordCountByIdInAnotherTransaction(3, 1, "Data should be visible after commit");

			properties = connection.getClientInfo();
			assertNull(properties.get("transaction_id"));
		}
	}

	@Test
	void shouldHandleSequentialTransactions() throws SQLException {
		try (Connection connection = createConnection();
			 Statement statement = connection.createStatement()) {
			
			connection.setAutoCommit(false);


			// First transaction
			statement.execute("INSERT INTO transaction_test VALUES (4, 'test')");
			checkRecordCountByIdInAnotherTransaction(4, 0, "Data should not be visible before commit");
			connection.commit();

			checkRecordCountByIdInAnotherTransaction(4, 1, "Data should be visible after commit");

			// Second transaction
			statement.execute("INSERT INTO transaction_test VALUES (5, 'test')");
			checkRecordCountByIdInAnotherTransaction(5, 0, "Data should not be visible before commit");
			connection.commit();

			checkRecordCountByIdInAnotherTransaction(4, 1, "Data should be visible after commit");
			checkRecordCountByIdInAnotherTransaction(5, 1, "Data should be visible after commit");

			// Should be successful without errors
            assertFalse(connection.getAutoCommit());
		}
	}

	@Test
	void shouldRollbackTransactionSuccessfully() throws SQLException {
		try (Connection connection = createConnection();
			 Statement statement = connection.createStatement()) {
			
			connection.setAutoCommit(false);
			
			// Start transaction
			statement.execute("INSERT INTO transaction_test VALUES (6, 'test')");

			Properties properties = connection.getClientInfo();
			assertNotNull(properties.get("transaction_id"));

			checkRecordCountByIdInAnotherTransaction(6, 0, "Data should not be visible before commit");

			// Rollback
			connection.rollback();

			checkRecordCountByIdInAnotherTransaction(6, 0, "Data should not be visible after rollback");

			properties = connection.getClientInfo();
			assertNull(properties.get("transaction_id"));
		}
	}

	@Test
	void shouldWorkWithPreparedStatements() throws SQLException {
		try (Connection connection = createConnection();
			 PreparedStatement ps = connection.prepareStatement("INSERT INTO transaction_test VALUES (?, 'test')")) {
			
			connection.setAutoCommit(false);
			
			ps.setInt(1, 7);
			ps.executeUpdate();

			checkRecordCountByIdInAnotherTransaction(7, 0, "Data should not be visible before commit");

			connection.commit();
			checkRecordCountByIdInAnotherTransaction(7, 1, "Data should be visible after commit");
		}
	}

	@Test
	void shouldNotCommitTransactionWhenConnectionClosesOnAutoCommitTrue() throws SQLException {
		try (Connection connection = createConnection();
			 Statement statement = connection.createStatement()) {

			statement.execute("BEGIN TRANSACTION;");

			statement.execute("INSERT INTO transaction_test VALUES (8, 'test')");
			checkRecordCountByIdInAnotherTransaction(8, 0, "Data should not be visible before commit");
		}

		checkRecordCountByIdInAnotherTransaction(8, 0, "Data should not be visible before commit");
	}

	@Test
	void shouldNotCommitTransactionWhenConnectionClosesOnAutoCommitFalse() throws SQLException {
		try (Connection connection = createConnection();
			 Statement statement = connection.createStatement()) {

			connection.setAutoCommit(false);

			statement.execute("INSERT INTO transaction_test VALUES (9, 'test')");
			checkRecordCountByIdInAnotherTransaction(9, 0, "Data should not be visible before commit");
		}

		checkRecordCountByIdInAnotherTransaction(9, 0, "Data should not be visible before commit");
	}

	@Test
	void shouldThrowExceptionWhenStartingTransactionDuringTransaction() throws SQLException {
		try (Connection connection = createConnection();
			 Statement statement = connection.createStatement()) {

			statement.execute("BEGIN TRANSACTION;");
			Properties properties = connection.getClientInfo();
			assertNotNull(properties.get("transaction_id"));

			SQLException exception = assertThrows(SQLException.class, () -> statement.execute("BEGIN TRANSACTION;"));
			assertTrue(exception.getMessage().contains("cannot BEGIN transaction: a transaction is already in progress"));
		}
	}

	@Test
	void shouldThrowExceptionWhenCommitingWithNoTransaction() throws SQLException {
		try (Connection connection = createConnection();
			 Statement statement = connection.createStatement()) {
			Properties properties = connection.getClientInfo();
			assertNull(properties.get("transaction_id"));

			SQLException exception = assertThrows(SQLException.class, () -> statement.execute("COMMIT;"));
			assertTrue(exception.getMessage().contains("cannot COMMIT transaction: no transaction is in progress"));
		}
	}

	@Test
	void shouldThrowExceptionWhenRollbackWithNoTransaction() throws SQLException {
		try (Connection connection = createConnection();
			 Statement statement = connection.createStatement()) {
			Properties properties = connection.getClientInfo();
			assertNull(properties.get("transaction_id"));

			SQLException exception = assertThrows(SQLException.class, () -> statement.execute("ROLLBACK;"));
			assertTrue(exception.getMessage().contains("Cannot ROLLBACK transaction: no transaction is in progress"));
		}
	}

	@Test
	void shouldCommitTableCreationAndDataInsertion() throws SQLException {
		String tableName = "transaction_commit_test";
		
		try (Connection connection = createConnection()) {
			try (Statement statement = connection.createStatement()) {
				statement.execute("DROP TABLE IF EXISTS " + tableName);
			}
			
			connection.setAutoCommit(false);
			
			String createTableSQL = String.format("CREATE TABLE %s (id INT, name STRING) PRIMARY INDEX id", tableName);
			String insertSQL = String.format("INSERT INTO %s (id, name) VALUES (0, 'some_text')", tableName);
			String checkTableSQL = String.format("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '%s'", tableName);
			String selectSQL = String.format("SELECT * FROM %s", tableName);
			
			try (Statement txStatement = connection.createStatement()) {
				txStatement.execute(createTableSQL);
				txStatement.execute(insertSQL);
			}
			
			try (Connection checkConnection = createConnection();
				 Statement checkStatement = checkConnection.createStatement();
				 ResultSet rs = checkStatement.executeQuery(checkTableSQL)) {
				
				assertTrue(rs.next());
				int count = rs.getInt(1);
				assertEquals(0, count, "Table transaction_commit_test already exists, but it shouldn't");
			}
			
			connection.commit();
			
			try (Statement statement = connection.createStatement();
				 ResultSet rs = statement.executeQuery(selectSQL)) {
				
				assertTrue(rs.next());
				int id = rs.getInt("id");
				String name = rs.getString("name");
				
				assertEquals(0, id, "id is not equal");
				assertEquals("some_text", name, "name is not equal");

                assertFalse(rs.next(), "Next() returned true when it shouldn't");
			}
		}
	}

	@Test
	void shouldRollbackTableCreationAndDataInsertion() throws SQLException {
		String tableName = "transaction_rollback_test";

		try (Connection connection = createConnection()) {
			try (Statement statement = connection.createStatement()) {
				statement.execute("DROP TABLE IF EXISTS " + tableName);
			}

			connection.setAutoCommit(false);

			String createTableSQL = String.format("CREATE TABLE %s (id INT, name STRING) PRIMARY INDEX id", tableName);
			String insertSQL = String.format("INSERT INTO %s (id, name) VALUES (0, 'some_text')", tableName);
			String checkTableSQL = String.format("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '%s'", tableName);

			try (Statement txStatement = connection.createStatement()) {
				txStatement.execute(createTableSQL);
				txStatement.execute(insertSQL);
			}

			try (Connection checkConnection = createConnection();
				 Statement checkStatement = checkConnection.createStatement();
				 ResultSet rs = checkStatement.executeQuery(checkTableSQL)) {

				assertTrue(rs.next());
				int count = rs.getInt(1);
				assertEquals(0, count, "Table transaction_rollback_test already exists, but it shouldn't");
			}

			connection.rollback();

			try (Statement statement = connection.createStatement();
				 ResultSet rs = statement.executeQuery(checkTableSQL)) {

				assertTrue(rs.next());
				int count = rs.getInt(1);
				assertEquals(0, count, "Table transaction_rollback_test exists, but it shouldn't");
			}
		}
	}

	@Test
	void shouldParallelTransactions() throws SQLException {
		String tableName = "parallel_transactions_test";
		String dropTableSQL = String.format("DROP TABLE IF EXISTS %s", tableName);
		String createTableSQL = String.format("CREATE TABLE IF NOT EXISTS %s (id INT, name STRING) PRIMARY INDEX id", tableName);
		String insertSQL = String.format("INSERT INTO %s (id, name) VALUES (?, ?)", tableName);
		String selectSQL = String.format("SELECT * FROM %s ORDER BY id", tableName);

		String firstName = "first";
		String secondName = "second";

		try (Connection tx1 = createConnection();
			 Connection tx2 = createConnection()) {

			try (Statement statement = tx1.createStatement()) {
				statement.execute(dropTableSQL);
				statement.execute(createTableSQL);
			}

			tx1.setAutoCommit(false);
			tx2.setAutoCommit(false);

			try (PreparedStatement ps1 = tx1.prepareStatement(insertSQL)) {
				ps1.setInt(1, 1);
				ps1.setString(2, firstName);
				ps1.executeUpdate();
			}

			try (PreparedStatement ps2 = tx2.prepareStatement(insertSQL)) {
				ps2.setInt(1, 2);
				ps2.setString(2, secondName);
				ps2.executeUpdate();
			}

			validateSingleResult(tx1, selectSQL, 1, firstName);
			validateSingleResult(tx2, selectSQL, 2, secondName);

			tx1.commit();
			tx2.commit();
		}
			
		try (Connection connection = createConnection();
			 Statement statement = connection.createStatement();
			 ResultSet rs = statement.executeQuery(selectSQL)) {

			assertTrue(rs.next());
			int id = rs.getInt("id");
			String name = rs.getString("name");
			assertEquals(1, id, "id1 is not equal after commit");
			assertEquals(firstName, name, "name1 is not equal after commit");

			assertTrue(rs.next());
			id = rs.getInt("id");
			name = rs.getString("name");
			assertEquals(2, id, "id2 is not equal after commit");
			assertEquals(secondName, name, "name2 is not equal after commit");

			assertFalse(rs.next(), "Next() returned true when it shouldn't after commit");
		}
	}
	
	private void validateSingleResult(Connection connection, String selectSQL, int expectedId, String expectedName) throws SQLException {
		try (Statement statement = connection.createStatement();
			 ResultSet rs = statement.executeQuery(selectSQL)) {
			
			assertTrue(rs.next());
			int id = rs.getInt("id");
			String name = rs.getString("name");
			assertEquals(expectedId, id);
			assertEquals(expectedName, name);
			assertTrue(!rs.next());
		}
	}

	private void checkRecordCountByIdInAnotherTransaction(Integer id, int expected, String message) throws SQLException {
		try (Connection connection2 = createConnection();
			 Statement checkStatement = connection2.createStatement();
			 ResultSet rs = checkStatement.executeQuery("SELECT COUNT(*) FROM transaction_test WHERE id = " + id)) {

			assertTrue(rs.next());
			int count = rs.getInt(1);
			assertEquals(expected, count, message);
		}
	}
}
