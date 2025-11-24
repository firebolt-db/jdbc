package integration.tests;

import com.firebolt.jdbc.FireboltBackendType;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.statement.preparedstatement.FireboltParquetStatement;
import com.firebolt.jdbc.testutils.TestTag;
import integration.IntegrationTest;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import lombok.CustomLog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@CustomLog
@Tag(TestTag.V2)
//@Tag(TestTag.CORE) //todo activate this when a core image supporting this feature will be released on ghcr
class ParquetStatementTest extends IntegrationTest {

	@TempDir
	Path tempDir;

	private String tableName;
	private byte[] parquetFileContent;

	/**
	 * Creates a connection based on the backend type.
	 * <p>
	 * If the backend type is V2 (CLOUD_2_0), uses the parquet_statement_test_engine
	 * system property if present, otherwise falls back to the default engine.
	 * If the backend type is Core (FIREBOLT_CORE), creates a normal connection with the default engine.
	 *
	 * @return a connection configured based on the backend type
	 * @throws SQLException if the connection cannot be established
	 */
	private Connection createParquetTestConnection() throws SQLException {
		FireboltBackendType backendType = getBackendType();
		if (backendType == FireboltBackendType.CLOUD_2_0) {
			// For V2, use parquet_statement_test_engine if present, otherwise default engine
			String parquetTestEngine = integration.ConnectionInfo.getInstance().getParquetStatementTestEngine();
			return createConnection(parquetTestEngine);
		} else {
			// For Core, use normal connection with default engine
			return createConnection();
		}
	}

	@BeforeEach
	void beforeEach() throws SQLException, IOException {
		// Load the parquet file content once for all tests
		parquetFileContent = loadParquetFileFromResources();
		
		// Create a test table matching the parquet file schema: id (int64), name (string)
		tableName = "parquet_test_" + System.currentTimeMillis();
		try (Connection connection = createParquetTestConnection();
			 var statement = connection.createStatement()) {
			statement.execute(String.format(
					"CREATE FACT TABLE IF NOT EXISTS %s (id LONG, name TEXT) PRIMARY INDEX id",
					tableName));
		}
	}

	@AfterEach
	void afterEach() throws SQLException {
		try (Connection connection = createParquetTestConnection();
			 var statement = connection.createStatement()) {
			statement.execute(String.format("DROP TABLE IF EXISTS %s", tableName));
		} catch (Exception e) {
			log.warn("Could not cleanup table", e);
		}
	}

	@Test
	void shouldExecuteUpdateWithByteArrayFiles() throws SQLException {
		try (FireboltConnection connection = createParquetTestConnection().unwrap(FireboltConnection.class)) {
			String sql = String.format("INSERT INTO %s SELECT id, name FROM read_parquet('upload://file1')", tableName);

			try (FireboltParquetStatement parquetStatement = connection.createParquetStatement()) {

				Map<String, byte[]> files = new HashMap<>();
				files.put("file1", parquetFileContent);

				// Execute the statement
				parquetStatement.executeUpdate(sql, files);
			}

			// Verify data was inserted
			validateRowCount(connection);
			
			// Verify the actual data matches the parquet file content
			validateParquetFileData(connection);
		}
	}

	@Test
	void shouldExecuteQueryWithMultipleFiles() throws SQLException {
		try (FireboltConnection connection = createParquetTestConnection().unwrap(FireboltConnection.class)) {
			String sql = String.format(
					"INSERT INTO %s SELECT id, name FROM read_parquet('upload://file1') UNION ALL SELECT id, name FROM read_parquet('upload://file2')",
					tableName);

			try (FireboltParquetStatement parquetStatement = connection.createParquetStatement()) {

				Map<String, byte[]> files = new HashMap<>();
				files.put("file1", parquetFileContent);
				files.put("file2", parquetFileContent); // Use same file twice for testing

				// Execute the statement
				parquetStatement.executeUpdate(sql, files);
			}

			// Verify data was inserted (should have double the rows since we used the same file twice)
			try (var statement = connection.createStatement();
				 ResultSet rs = statement.executeQuery(String.format("SELECT COUNT(*) FROM %s", tableName))) {
				assertTrue(rs.next());
				int count = rs.getInt(1);
				assertEquals(6, count, "Expected 6 rows from two parquet files (3 rows each)");
			}
		}
	}

	@Test
	void shouldThrowExceptionWhenFilesMapIsNull() throws SQLException {
		try (FireboltConnection connection = createParquetTestConnection().unwrap(FireboltConnection.class)) {
			String sql = String.format("INSERT INTO %s SELECT * FROM read_parquet('upload://file1')", tableName);

			try (FireboltParquetStatement parquetStatement = connection.createParquetStatement()) {

				FireboltException exception = assertThrows(FireboltException.class,
						() -> parquetStatement.executeUpdate(sql, (Map<String, byte[]>) null));
				assertTrue(exception.getMessage().contains("Files map cannot be null or empty"));
			}
		}
	}

	@Test
	void shouldThrowExceptionWhenFilesMapIsEmpty() throws SQLException {
		try (FireboltConnection connection = createParquetTestConnection().unwrap(FireboltConnection.class)) {
			String sql = String.format("INSERT INTO %s SELECT * FROM read_parquet('upload://file1')", tableName);

			try (FireboltParquetStatement parquetStatement = connection.createParquetStatement()) {

				Map<String, byte[]> emptyFiles = new HashMap<>();
				FireboltException exception = assertThrows(FireboltException.class,
						() -> parquetStatement.executeUpdate(sql, emptyFiles));
				assertTrue(exception.getMessage().contains("Files map cannot be null or empty"));
			}
		}
	}

	@Test
	void shouldThrowExceptionWhenFileIdentifierIsNull() throws SQLException {
		try (FireboltConnection connection = createParquetTestConnection().unwrap(FireboltConnection.class)) {
			String sql = String.format("INSERT INTO %s SELECT * FROM read_parquet('upload://file1')", tableName);

			try (FireboltParquetStatement parquetStatement = connection.createParquetStatement()) {

				Map<String, byte[]> files = new HashMap<>();
				files.put(null, new byte[]{1, 2, 3});

				FireboltException exception = assertThrows(FireboltException.class,
						() -> parquetStatement.executeUpdate(sql, files));
				assertTrue(exception.getMessage().contains("File identifier cannot be null"));
			}
		}
	}

	@Test
	void shouldThrowExceptionWhenFileContentIsNull() throws SQLException {
		try (FireboltConnection connection = createParquetTestConnection().unwrap(FireboltConnection.class)) {
			String sql = String.format("INSERT INTO %s SELECT * FROM read_parquet('upload://file1')", tableName);

			try (FireboltParquetStatement parquetStatement = connection.createParquetStatement()) {

				Map<String, byte[]> files = new HashMap<>();
				files.put("file1", null);

				FireboltException exception = assertThrows(FireboltException.class,
						() -> parquetStatement.executeUpdate(sql, files));
				assertTrue(exception.getMessage().contains("File content for identifier 'file1' cannot be null"));
			}
		}
	}

	@Test
	void shouldExecuteWithByteArraysForInsert() throws SQLException {
		try (FireboltConnection connection = createParquetTestConnection().unwrap(FireboltConnection.class)) {
			String sql = String.format("INSERT INTO %s SELECT id, name FROM read_parquet('upload://file1')", tableName);

			try (FireboltParquetStatement parquetStatement = connection.createParquetStatement()) {

				Map<String, byte[]> files = new HashMap<>();
				files.put("file1", parquetFileContent);

				// Execute the statement (should return false for INSERT)
				boolean hasResultSet = parquetStatement.execute(sql, files);
				assertFalse(hasResultSet, "INSERT should not return a ResultSet");
			}

			// Verify data was inserted
			validateRowCount(connection);
			validateParquetFileData(connection);
		}
	}

	@Test
	void shouldExecuteWithByteArraysForSelect() throws SQLException {
		try (FireboltConnection connection = createParquetTestConnection().unwrap(FireboltConnection.class)) {
			String sql = "SELECT id, name FROM read_parquet('upload://file1') LIMIT 10";

			try (FireboltParquetStatement parquetStatement = connection.createParquetStatement()) {

				Map<String, byte[]> files = new HashMap<>();
				files.put("file1", parquetFileContent);

				// Execute the statement (should return true for SELECT)
				boolean hasResultSet = parquetStatement.execute(sql, files);
				assertTrue(hasResultSet, "SELECT should return a ResultSet");

				// Validate the result set
				try (ResultSet rs = parquetStatement.getResultSet()) {
					validateParquetFileResultSet(rs);
				}
			}
		}
	}

	/**
	 * Validates that exactly 3 rows were inserted from the parquet file
	 *
	 * @param connection the database connection
	 * @throws SQLException if there's an error querying the database
	 */
	private void validateRowCount(FireboltConnection connection) throws SQLException {
		try (var statement = connection.createStatement();
			 ResultSet rs = statement.executeQuery(String.format("SELECT COUNT(*) FROM %s", tableName))) {
			assertTrue(rs.next());
			int count = rs.getInt(1);
			assertEquals(3, count, "Expected 3 rows from parquet file (Alice, Bob, Charlie)");
		}
	}

	/**
	 * Validates that the table contains exactly the 3 rows from the parquet file with correct values
	 *
	 * @param connection the database connection
	 * @throws SQLException if there's an error querying the database
	 */
	private void validateParquetFileData(FireboltConnection connection) throws SQLException {
		try (var statement = connection.createStatement();
			 ResultSet rs = statement.executeQuery(String.format("SELECT id, name FROM %s ORDER BY id", tableName))) {
			// Verify first row: id=1, name=Alice
			assertTrue(rs.next(), "Expected first row");
			assertEquals(1L, rs.getLong("id"));
			assertEquals("Alice", rs.getString("name"));
			
			// Verify second row: id=2, name=Bob
			assertTrue(rs.next(), "Expected second row");
			assertEquals(2L, rs.getLong("id"));
			assertEquals("Bob", rs.getString("name"));
			
			// Verify third row: id=3, name=Charlie
			assertTrue(rs.next(), "Expected third row");
			assertEquals(3L, rs.getLong("id"));
			assertEquals("Charlie", rs.getString("name"));
			
			// Verify no more rows
			assertFalse(rs.next(), "Expected exactly 3 rows");
		}
	}

	/**
	 * Validates that the ResultSet contains exactly the 3 rows from the parquet file with correct values
	 *
	 * @param rs the ResultSet to validate
	 * @throws SQLException if there's an error reading the ResultSet
	 */
	private void validateParquetFileResultSet(ResultSet rs) throws SQLException {
		assertNotNull(rs);
		// Verify first row: id=1, name=Alice
		assertTrue(rs.next(), "Expected first row");
		assertEquals(1L, rs.getLong("id"));
		assertEquals("Alice", rs.getString("name"));
		
		// Verify second row: id=2, name=Bob
		assertTrue(rs.next(), "Expected second row");
		assertEquals(2L, rs.getLong("id"));
		assertEquals("Bob", rs.getString("name"));
		
		// Verify third row: id=3, name=Charlie
		assertTrue(rs.next(), "Expected third row");
		assertEquals(3L, rs.getLong("id"));
		assertEquals("Charlie", rs.getString("name"));
		
		// Verify no more rows
		assertFalse(rs.next(), "Expected exactly 3 rows");
	}

	/**
	 * Loads the parquet file from test resources.
	 *
	 * @return the parquet file content as byte array
	 * @throws IOException if the file cannot be read
	 */
	private byte[] loadParquetFileFromResources() throws IOException {
		try (InputStream is = getClass().getResourceAsStream("/parquet/id_name_test.parquet")) {
			assertNotNull(is, "Parquet file not found in resources: /parquet/id_name_test.parquet");
			return is.readAllBytes();
		}
	}
}

