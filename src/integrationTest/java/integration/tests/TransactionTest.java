package integration.tests;

import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.testutils.TestTag;
import integration.IntegrationTest;
import lombok.CustomLog;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@CustomLog
@Tag(TestTag.V2)
class TransactionTest extends IntegrationTest {

	@Test
	void shouldRemoveTransactionIdOnCommit() throws SQLException {
		try (FireboltConnection connection = createConnection().unwrap(FireboltConnection.class);
			 Statement statement = connection.createStatement()) {
			statement.execute("BEGIN TRANSACTION;");
			FireboltProperties fireboltProperties = connection.getSessionProperties();
			assertNotNull(fireboltProperties.getRuntimeAdditionalProperties().get("transaction_id"));
			statement.execute("COMMIT;");
			fireboltProperties = connection.getSessionProperties();
			assertNull(fireboltProperties.getRuntimeAdditionalProperties().get("transaction_id"));
		}
	}

	@Test
	void shouldRemoveTransactionIdOnRollback() throws SQLException {
		try (FireboltConnection connection = createConnection().unwrap(FireboltConnection.class);
			 Statement statement = connection.createStatement()) {
			statement.execute("BEGIN TRANSACTION;");
			FireboltProperties fireboltProperties = connection.getSessionProperties();
			assertNotNull(fireboltProperties.getRuntimeAdditionalProperties().get("transaction_id"));
			statement.execute("ROLLBACK;");
			fireboltProperties = connection.getSessionProperties();
			assertNull(fireboltProperties.getRuntimeAdditionalProperties().get("transaction_id"));
		}
	}

	@Test
	void shouldCommitTransactionWhenSwitchingToAutoCommit() throws SQLException {
		try (FireboltConnection connection = createConnection().unwrap(FireboltConnection.class);
			 Statement statement = connection.createStatement()) {
			
			connection.setAutoCommit(false);
			
			statement.execute("SELECT 1");
			
			FireboltProperties properties = connection.getSessionProperties();
			assertNotNull(properties.getRuntimeAdditionalProperties().get("transaction_id"));
			
			connection.setAutoCommit(true);
			
			properties = connection.getSessionProperties();
			assertNull(properties.getRuntimeAdditionalProperties().get("transaction_id"));
		}
	}

	@Test
	void shouldHandleSequentialTransactions() throws SQLException {
		try (FireboltConnection connection = createConnection().unwrap(FireboltConnection.class);
			 Statement statement = connection.createStatement()) {
			
			connection.setAutoCommit(false);
			
			// First transaction
			statement.execute("SELECT 1");
			connection.commit();
			
			// Second transaction
			statement.execute("SELECT 2");
			connection.commit();
			
			// Should be successful without errors
			assertTrue(connection.getAutoCommit() == false);
		}
	}

	@Test
	void shouldRollbackTransactionSuccessfully() throws SQLException {
		try (FireboltConnection connection = createConnection().unwrap(FireboltConnection.class);
			 Statement statement = connection.createStatement()) {
			
			connection.setAutoCommit(false);
			
			// Start transaction
			statement.execute("SELECT 1");
			
			FireboltProperties properties = connection.getSessionProperties();
			assertNotNull(properties.getRuntimeAdditionalProperties().get("transaction_id"));
			
			// Rollback
			connection.rollback();
			
			properties = connection.getSessionProperties();
			assertNull(properties.getRuntimeAdditionalProperties().get("transaction_id"));
		}
	}

	@Test
	void shouldWorkWithPreparedStatements() throws SQLException {
		try (FireboltConnection connection = createConnection().unwrap(FireboltConnection.class);
			 PreparedStatement ps = connection.prepareStatement("SELECT ?")) {
			
			connection.setAutoCommit(false);
			
			ps.setInt(1, 42);
			try (ResultSet rs = ps.executeQuery()) {
				assertTrue(rs.next());
				assertEquals(42, rs.getInt(1));
			}
			
			connection.commit();
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
		
		try (Connection tx1 = createConnection();
			 Connection tx2 = createConnection()) {
			String dropTableSQL = String.format("DROP TABLE IF EXISTS %s", tableName);
			String createTableSQL = String.format("CREATE TABLE IF NOT EXISTS %s (id INT, name STRING) PRIMARY INDEX id", tableName);
			String insertSQL = String.format("INSERT INTO %s (id, name) VALUES (?, ?)", tableName);
			String selectSQL = String.format("SELECT * FROM %s ORDER BY id", tableName);
			
			try (Statement statement = tx1.createStatement()) {
				statement.execute(dropTableSQL);
				statement.execute(createTableSQL);
			}
			
			tx1.setAutoCommit(false);
			tx2.setAutoCommit(false);

			String firstName = "first";
			String secondName = "second";
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
}
