package integration.tests;

import com.firebolt.jdbc.exception.FireboltException;
import integration.ConnectionInfo;
import integration.IntegrationTest;
import lombok.CustomLog;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@CustomLog
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SystemEngineTest extends IntegrationTest {

	private static final long ID = ProcessHandle.current().pid() + System.currentTimeMillis();
	private static final String SECOND_DATABASE_NAME = "jdbc_system_engine_integration_test_" + ID;
	private static final String ENGINE_NAME = SECOND_DATABASE_NAME + "_engine";
	private static final String ENGINE_NEW_NAME = ENGINE_NAME + "_2";

	private static final String USE_DATABASE_NAME = "jdbc_use_db_" + ID;
	private static final String TABLE = USE_DATABASE_NAME + "_table";
	private static final String TABLE1 = TABLE + "_1";
	private static final String TABLE2 = TABLE + "_2";

	@BeforeAll
	void beforeAll() {
		try {
			executeStatementFromFile("/statements/system/ddl.sql", getSystemEngineName());
		} catch (Exception e) {
			log.warn("Could not execute statement", e);
		}
	}

	@AfterAll
	void afterAll() {
		try {
			executeStatementFromFile("/statements/system/cleanup.sql", getSystemEngineName());
		} catch (Exception e) {
			log.warn("Could not execute statement", e);
		}
	}

	@Test
	void shouldSelect1() throws SQLException {
		try (Connection connection = createConnection();
			 ResultSet rs = connection.createStatement().executeQuery("SELECT 1")) {
			assertTrue(rs.next());
			assertEquals(1, rs.getInt(1));
			assertFalse(rs.next());
		}
	}

	@Test
	@Tag("staging")
	void ddlFailure() throws SQLException {
		Collection<String> expectedErrorMessages = Set.of(
				"Cannot execute a DDL query on the system engine.",
				"std::invalid_argument: URI segment 'N/A' can't contain '/'");

		try (Connection connection = createConnection(getSystemEngineName())) {
			String errorMessage = assertThrows(
					FireboltException.class,
					() -> connection.createStatement().executeUpdate("CREATE DIMENSION TABLE dummy(id INT)"))
					.getErrorMessageFromServer().replaceAll("\r?\n", "");
			assertTrue(expectedErrorMessages.contains(errorMessage));
		}
	}

	@Test
	@Tag("dev")
	void ddlSuccess() throws SQLException {
		try (Connection connection = createConnection(getSystemEngineName())) {
			connection.createStatement().executeUpdate("CREATE DIMENSION TABLE dummy(id INT)");
			connection.createStatement().executeUpdate("DROP TABLE dummy");
		}
	}

	@Test
	void shouldFailToSelectFromCustomDbUsingSystemEngine() throws SQLException {
		ConnectionInfo current = integration.ConnectionInfo.getInstance();
		String systemEngineJdbcUrl = new ConnectionInfo(current.getPrincipal(), current.getSecret(),
				current.getEnv(), current.getDatabase(), current.getAccount(), getSystemEngineName(), current.getApi()).toJdbcUrl();
		String customEngineJdbcUrl = current.toJdbcUrl();
		String principal = current.getPrincipal();
		String secret = current.getSecret();
		Collection<String> expectedErrorMessages = Set.of(
				"Queries against table dummy require a user engine",
				"The system engine doesn't support queries against table dummy. Run this query on a user engine.");

		try (Connection systemConnection = DriverManager.getConnection(systemEngineJdbcUrl, principal, secret);
			 Connection customConnection = DriverManager.getConnection(customEngineJdbcUrl, principal, secret)) {

			try {
				customConnection.createStatement().executeUpdate("CREATE DIMENSION TABLE dummy(id INT)");
				try (ResultSet rs = customConnection.createStatement().executeQuery("select count(*) from dummy")) {
					assertTrue(rs.next());
					assertEquals(0, rs.getInt(1));
					assertFalse(rs.next());
				}
				FireboltException e = assertThrows(FireboltException.class, () -> systemConnection.createStatement().executeQuery("select count(*) from dummy"));
				String actualErrorMessage = e.getErrorMessageFromServer().replaceAll("\r?\n", "");
				assertTrue(expectedErrorMessages.contains(actualErrorMessage));
			} finally {
				try {
					customConnection.createStatement().executeUpdate("DROP TABLE dummy");
				} catch(SQLException e) {
					// ignore the exception here; even if it happens it does not matter.
				}

			}
		}
	}

	@Test
	@Tag("v2")
	@Tag("dev")
	void useSuccess() throws SQLException {
		ConnectionInfo current = integration.ConnectionInfo.getInstance();
		try (Connection connection = createConnection(getSystemEngineName())) {
			try {
				connection.createStatement().executeUpdate(format("USE %s", current.getDatabase())); // use current DB; shouldn't have any effect
				assertNull(getTableDbName(connection, TABLE1)); // the table does not exist yet
				connection.createStatement().executeUpdate(format("CREATE TABLE %s ( id LONG)", TABLE1)); // create table1 in current DB
				assertEquals(current.getDatabase(), getTableDbName(connection, TABLE1)); // now table t1 exists
				Assert.assertThrows(SQLException.class, () -> connection.createStatement().executeUpdate(format("USE %s", USE_DATABASE_NAME))); // DB does not exist
				connection.createStatement().executeUpdate(format("CREATE DATABASE IF NOT EXISTS %s", USE_DATABASE_NAME)); // create DB
				connection.createStatement().executeUpdate(format("USE %s", USE_DATABASE_NAME)); // Now this should succeed
				connection.createStatement().executeUpdate(format("CREATE TABLE %s ( id LONG)", TABLE2)); // create table2 in other DB
				assertNull(getTableDbName(connection, TABLE1)); // table1 does not exist here
				assertEquals(USE_DATABASE_NAME, getTableDbName(connection, TABLE2)); // but table2 does exist
			} finally {
				// now clean up everything
				for (String query : new String[] {
						format("USE %s", USE_DATABASE_NAME), // switch to DB that should be current just in case because the previous code can fail at any phase
						format("DROP TABLE %s", TABLE2),
						format("DROP DATABASE %s", USE_DATABASE_NAME),
						format("USE %s", current.getDatabase()), // now switch back
						format("DROP TABLE %s", TABLE1)}) {
					try (Statement statement = connection.createStatement()) {
						statement.executeUpdate(query);
					} catch (SQLException e) { // catch just in case to do our best to clean everything even if test has failed
						log.warn("Cannot perform query {}",  query, e);
					}
				}
			}
		}
	}

	@Test
	@Tag("v2")
	@Tag("staging")
	void useFailure() throws SQLException {
		ConnectionInfo current = integration.ConnectionInfo.getInstance();
		try (Connection connection = createConnection(getSystemEngineName())) {
			try {
				Assert.assertThrows(SQLException.class, () -> connection.createStatement().executeUpdate(format("USE %s", current.getDatabase()))); // unsupported and DB does not exist
				Assert.assertThrows(SQLException.class, () -> connection.createStatement().executeUpdate(format("USE %s", USE_DATABASE_NAME))); // DB does not exist
				connection.createStatement().executeUpdate(format("CREATE DATABASE IF NOT EXISTS %s", USE_DATABASE_NAME)); // create DB
				Assert.assertThrows(SQLException.class, () -> connection.createStatement().executeUpdate(format("USE %s", USE_DATABASE_NAME))); // DB exists but use statement is unsupported anyway
			} finally {
				try (Statement statement = connection.createStatement()) {
					statement.executeUpdate(format("DROP DATABASE %s", USE_DATABASE_NAME));
				} catch (SQLException e) { // catch just in case to do our best to clean everything even if test has failed
					log.warn("Cannot drop database ",  USE_DATABASE_NAME, e);
				}
			}
		}
	}

	private String getTableDbName(Connection connection, String table) throws SQLException {
		try (PreparedStatement ps = connection.prepareStatement("select table_catalog from information_schema.tables where table_name=?")) {
			ps.setString(1, table);
			try (ResultSet rs = ps.executeQuery()) {
				return rs.next() ? rs.getString(1) : null;
			}
		}
	}

	@Test
	@Tag("slow")
	void shouldExecuteEngineManagementQueries() throws SQLException {
		try (Connection connection = this.createConnection(getSystemEngineName())) {
			List<String> queries = Arrays.asList(format("CREATE DATABASE IF NOT EXISTS %s", SECOND_DATABASE_NAME),
					format("CREATE ENGINE %s", ENGINE_NAME),
					format("ATTACH ENGINE %s TO %s;", ENGINE_NAME, SECOND_DATABASE_NAME),
					format("ALTER DATABASE %s WITH DESCRIPTION = 'JDBC Integration test'", SECOND_DATABASE_NAME),
					format("ALTER ENGINE %s RENAME TO %s", ENGINE_NAME, ENGINE_NEW_NAME),
					format("START ENGINE %s", ENGINE_NEW_NAME), format("STOP ENGINE %s", ENGINE_NEW_NAME),
					format("DROP ENGINE %s", ENGINE_NEW_NAME), format("DROP DATABASE %s", SECOND_DATABASE_NAME));
			executeAll(connection, queries);
		}
	}

	private void executeAll(Connection connection, Iterable<String> queries) throws SQLException {
		for (String query : queries) {
			try (Statement statement = connection.createStatement()) {
				statement.execute(query);
			}
		}
	}
}
