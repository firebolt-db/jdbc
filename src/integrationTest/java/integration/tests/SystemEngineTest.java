package integration.tests;

import com.firebolt.jdbc.connection.CacheListener;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.testutils.TestTag;
import integration.ConnectionInfo;
import integration.EnvironmentCondition;
import integration.IntegrationTest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import lombok.CustomLog;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static com.firebolt.jdbc.connection.FireboltConnectionUserPassword.SYSTEM_ENGINE_NAME;
import static integration.EnvironmentCondition.Attribute.databaseVersion;
import static java.lang.String.format;
import static java.util.Map.entry;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@CustomLog
@Tag(TestTag.V2)
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
		try (Connection connection = createConnection("");
			 ResultSet rs = connection.createStatement().executeQuery("SELECT 1")) {
			assertTrue(rs.next());
			assertEquals(1, rs.getInt(1));
			assertFalse(rs.next());
		}
	}

	@Test
	void shouldThrowExceptionWhenExecutingWrongQuery() throws SQLException {
		try (Connection connection = createConnection(getSystemEngineName()); Statement statement = connection.createStatement()) {
			String errorMessage = assertThrows(FireboltException.class, () -> statement.executeQuery("select wrong query")).getMessage();
			assertTrue(errorMessage.contains("Column 'wrong' does not exist."));
		}
	}

	@Test
	@EnvironmentCondition(value = "4.2.0", attribute = databaseVersion, comparison = EnvironmentCondition.Comparison.GE)
	void shouldThrowExceptionWhenExecutingWrongQueryWithJsonError() throws SQLException {
		try (Connection connection = createConnection(getSystemEngineName()); Statement statement = connection.createStatement()) {
			statement.execute("set advanced_mode=1");
			statement.execute("set enable_json_error_output_format=true");
			String errorMessage = assertThrows(FireboltException.class, () -> statement.executeQuery("select wrong query")).getMessage();
			assertTrue(errorMessage.contains("Column 'wrong' does not exist."));
		}
	}

	@Test
	void shouldFailToSelectFromCustomDbUsingSystemEngine() throws SQLException {
		ConnectionInfo current = integration.ConnectionInfo.getInstance();
		String systemEngineJdbcUrl = new ConnectionInfo(current.getPrincipal(), current.getSecret(),
				current.getEnv(), current.getDatabase(), current.getAccount(), getSystemEngineName(),
				current.getApi()).toJdbcUrl();
		String customEngineJdbcUrl = current.toJdbcUrl();
		String principal = current.getPrincipal();
		String secret = current.getSecret();
		Collection<String> expectedErrorMessages = Set.of(
				"Queries against table dummy require a user engine",
				"The system engine doesn't support queries against table dummy. Run this query on a user engine.",
				"relation \"dummy\" does not exist");

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
				// Check that at least  one error message from expectedErrorMessages is contained in the actual error message
				assertTrue(expectedErrorMessages.stream().anyMatch(actualErrorMessage::contains), "Unexpected error message: " + actualErrorMessage);
			} finally {
				try {
					customConnection.createStatement().executeUpdate("DROP TABLE dummy");
				} catch(SQLException e) {
					// ignore the exception here; even if it happens it does not matter.
				}

			}
		}
	}

	@ParameterizedTest
	@ValueSource(strings = {"", "DATABASE"})
	@EnvironmentCondition(value = "2", comparison = EnvironmentCondition.Comparison.GE)
	void useDatabase(String entityType) throws SQLException {
		ConnectionInfo current = integration.ConnectionInfo.getInstance();
		try (Connection connection = createConnection(getSystemEngineName())) {
			try {
				connection.createStatement().executeUpdate(format("USE %s \"%s\"", entityType, current.getDatabase())); // use current DB; shouldn't have any effect
				assertNull(getTableDbName(connection, TABLE1)); // the table does not exist yet
				connection.createStatement().executeUpdate(format("CREATE TABLE \"%s\" ( id LONG)", TABLE1)); // create table1 in current DB
				assertEquals(current.getDatabase(), getTableDbName(connection, TABLE1)); // now table t1 exists
				Assert.assertThrows(SQLException.class, () -> connection.createStatement().executeUpdate(format("USE %s %s", entityType, USE_DATABASE_NAME))); // DB does not exist
				connection.createStatement().executeUpdate(format("CREATE DATABASE IF NOT EXISTS \"%s\"", USE_DATABASE_NAME)); // create DB
				connection.createStatement().executeUpdate(format("USE %s %s", entityType, USE_DATABASE_NAME)); // Now this should succeed
				connection.createStatement().executeUpdate(format("CREATE TABLE \"%s\" ( id LONG)", TABLE2)); // create table2 in other DB
				assertNull(getTableDbName(connection, TABLE1)); // table1 does not exist here
				assertEquals(USE_DATABASE_NAME, getTableDbName(connection, TABLE2)); // but table2 does exist
			} finally {
				// now clean up everything
				for (String query : new String[] {
						format("USE %s \"%s\"", entityType, USE_DATABASE_NAME), // switch to DB that should be current just in case because the previous code can fail at any phase
						format("DROP TABLE %s", TABLE2),
						format("DROP DATABASE \"%s\"", USE_DATABASE_NAME),
						format("USE %s \"%s\"", entityType, current.getDatabase()), // now switch back
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
	@Tag(TestTag.SLOW)
	@EnvironmentCondition(value = "2", comparison = EnvironmentCondition.Comparison.GE)
	void useEngine() throws SQLException {
		try (Connection connection = createConnection(getSystemEngineName())) {
			try {
				connection.createStatement().executeUpdate(format("USE ENGINE \"%s\"", SYSTEM_ENGINE_NAME));
				assertThrows(SQLException.class, () -> connection.createStatement().executeUpdate(format("USE ENGINE \"%s\"", ENGINE_NAME)));
				connection.createStatement().executeUpdate(format("CREATE ENGINE \"%s\"", ENGINE_NAME));
				connection.createStatement().executeUpdate(format("USE ENGINE \"%s\"", ENGINE_NAME));
				connection.createStatement().executeUpdate(format("CREATE DATABASE IF NOT EXISTS \"%s\"", USE_DATABASE_NAME));
				connection.createStatement().executeUpdate(format("USE DATABASE \"%s\"", USE_DATABASE_NAME));
				connection.createStatement().executeUpdate(format("CREATE TABLE \"%s\" ( id LONG)", TABLE1));
				connection.createStatement().executeUpdate(format("INSERT INTO %s (id) VALUES (1)", TABLE1)); // should succeed using user engine
				// switch back to the system engine
				connection.createStatement().executeUpdate(format("USE ENGINE \"%s\"", SYSTEM_ENGINE_NAME));
				assertThrows(SQLException.class, () -> connection.createStatement().executeUpdate(format("INSERT INTO %s (id) VALUES (1)", TABLE1))); // system engine cannot insert data
			} finally {
				connection.createStatement().executeUpdate(format("USE DATABASE \"%s\"", USE_DATABASE_NAME));
				connection.createStatement().executeUpdate(format("DROP TABLE %s", TABLE1));
				connection.createStatement().executeUpdate(format("DROP DATABASE \"%s\"", USE_DATABASE_NAME));
				connection.createStatement().executeUpdate(format("STOP ENGINE \"%s\"", ENGINE_NAME));
				connection.createStatement().executeUpdate(format("DROP ENGINE \"%s\"", ENGINE_NAME));
			}
		}
	}

	@Test
	@Tag(TestTag.SLOW)
	@EnvironmentCondition(value = "2", comparison = EnvironmentCondition.Comparison.GE)
	void useEngineMixedCase() throws SQLException {
		String mixedCaseEngineName = "JavaIntegrationTestMixedCase" + ID;
		try (Connection connection = createConnection(getSystemEngineName())) {
			try {
				connection.createStatement().executeUpdate(format("USE ENGINE \"%s\"", SYSTEM_ENGINE_NAME));
				connection.createStatement().executeUpdate(format("CREATE ENGINE \"%s\"", mixedCaseEngineName));
				connection.createStatement().executeUpdate(format("USE ENGINE \"%s\"", mixedCaseEngineName));
				assertThrows(SQLException.class, () -> connection.createStatement().executeUpdate(format("USE ENGINE %s", mixedCaseEngineName)));
			} finally {
				connection.createStatement().executeUpdate(format("USE ENGINE \"%s\"", SYSTEM_ENGINE_NAME));
				connection.createStatement().executeUpdate(format("STOP ENGINE \"%s\"", mixedCaseEngineName));
				connection.createStatement().executeUpdate(format("DROP ENGINE \"%s\"", mixedCaseEngineName));
			}
		}
	}

	@Test
	@Tag(TestTag.SLOW)
	@EnvironmentCondition(value = "2", comparison = EnvironmentCondition.Comparison.GE)
	void useEngineMixedCaseToLowerCase() throws SQLException {
		String mixedCaseEngineName = "JavaIntegrationTestToLowerCase" + ID;
		try (Connection connection = createConnection(getSystemEngineName())) {
			try {
				connection.createStatement().executeUpdate(format("USE ENGINE \"%s\"", SYSTEM_ENGINE_NAME));
				// engine name is lower cased because it is not quoted
				connection.createStatement().executeUpdate(format("CREATE ENGINE %s", mixedCaseEngineName));
				connection.createStatement().executeUpdate(format("USE ENGINE %s", mixedCaseEngineName));
				// engine name remains mixed case and statement fails because engine name was not quoted when we created the engine
				assertThrows(SQLException.class, () -> connection.createStatement().executeUpdate(format("USE ENGINE \"%s\"", mixedCaseEngineName)));
			} finally {
				connection.createStatement().executeUpdate(format("USE ENGINE \"%s\"", SYSTEM_ENGINE_NAME));
				connection.createStatement().executeUpdate(format("STOP ENGINE %s", mixedCaseEngineName));
				connection.createStatement().executeUpdate(format("DROP ENGINE %s", mixedCaseEngineName));
			}
		}
	}

	@Test
	void connectToAccountWithoutUser() throws SQLException {
		ConnectionInfo current = integration.ConnectionInfo.getInstance();
		String database = current.getDatabase();
		String serviceAccountName = format("%s_%d_sa_no_user", database, System.currentTimeMillis());
		try (Connection connection = createConnection(getSystemEngineName())) {
			try {
				connection.createStatement().executeUpdate(format("CREATE SERVICE ACCOUNT \"%s\" WITH DESCRIPTION = 'Ecosytem test with no user'", serviceAccountName));
				// This what I want to do here
				ResultSet genKeyRs = connection.createStatement().executeQuery(format("CALL fb_GENERATESERVICEACCOUNTKEY('%s')", serviceAccountName));
				assertTrue(genKeyRs.next());
				String clientId = genKeyRs.getString(2);
				String clientSecret = genKeyRs.getString(3);

				String jdbcUrl = format("jdbc:firebolt:%s?env=%s&account=%s&engine=%s", database, current.getEnv(), current.getAccount(), current.getEngine());

				((CacheListener)connection).cleanup();
				SQLException e = assertThrows(SQLException.class, () -> DriverManager.getConnection(jdbcUrl, clientId, clientSecret));
				assertTrue(e.getMessage().matches(format("Account '%s' does not exist in this organization or is not authorized.+RBAC.+", current.getAccount())), "Unexpected exception message: " + e.getMessage());
			} finally {
				connection.createStatement().executeUpdate(format("DROP SERVICE ACCOUNT \"%s\"", serviceAccountName));
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
	@Tag(TestTag.SLOW)
	void shouldExecuteEngineManagementQueries() throws SQLException {
		try (Connection connection = createConnection(getSystemEngineName())) {
			try {
				boolean attachEngineToDb = ((FireboltConnection)connection).getInfraVersion() < 2;
				List<String> queries = Stream.of(
								entry(true, format("CREATE DATABASE IF NOT EXISTS \"%s\"", SECOND_DATABASE_NAME)),
								entry(true, format("CREATE ENGINE \"%s\"", ENGINE_NAME)),
								entry(attachEngineToDb, format("ATTACH ENGINE \"%s\" TO \"%s\";", ENGINE_NAME, SECOND_DATABASE_NAME)),
								entry(true, format("ALTER DATABASE \"%s\" SET DESCRIPTION = 'JDBC Integration test'", SECOND_DATABASE_NAME)),
								entry(true, format("ALTER ENGINE \"%s\" RENAME TO \"%s\"", ENGINE_NAME, ENGINE_NEW_NAME)),
								entry(true, format("START ENGINE \"%s\"", ENGINE_NEW_NAME)))
						.filter(Map.Entry::getKey).map(Map.Entry::getValue).collect(toList());
				executeAll(connection, queries);
			} finally {
				// now clean up everything
				for (String query : new String[]{
						format("STOP ENGINE \"%s\"", ENGINE_NEW_NAME),
						format("DROP ENGINE \"%s\"", ENGINE_NEW_NAME),
						format("DROP DATABASE \"%s\"", SECOND_DATABASE_NAME)}) {
					try (Statement statement = connection.createStatement()) {
						statement.executeUpdate(query);
					} catch (
							SQLException e) { // catch just in case to do our best to clean everything even if test has failed
						log.warn("Cannot perform query {}", query, e);
					}
				}
			}
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
