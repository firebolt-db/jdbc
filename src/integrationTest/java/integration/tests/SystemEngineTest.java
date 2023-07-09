package integration.tests;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.*;

import integration.IntegrationTest;
import lombok.CustomLog;

@CustomLog
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SystemEngineTest extends IntegrationTest {

	private static final String DATABASE_NAME = "jdbc_system_engine_integration_test";
	private static final String ENGINE_NAME = "jdbc_system_engine_integration_test_engine";
	private static final String ENGINE_NEW_NAME = "jdbc_system_engine_integration_test_engine_2";
	private static final String SYSTEM_ENGINE_NAME = "system";

	@BeforeAll
	void beforeAll() {
		try {
			executeStatementFromFile("/statements/system/ddl.sql", SYSTEM_ENGINE_NAME);
		} catch (Exception e) {
			log.warn("Could not execute statement", e);
		}
	}

	@AfterAll
	void afterAll() {
		try {
			executeStatementFromFile("/statements/system/cleanup.sql", SYSTEM_ENGINE_NAME);
		} catch (Exception e) {
			log.warn("Could not execute statement", e);
		}
	}

	@Test
	void shouldExecuteQueriesUsingSystemEngine() throws SQLException {
		try (Connection connection = this.createConnection(SYSTEM_ENGINE_NAME)) {
			List<String> queries = Arrays.asList(String.format("CREATE DATABASE IF NOT EXISTS %s", DATABASE_NAME),
					String.format("CREATE ENGINE %s", ENGINE_NAME),
					String.format("ATTACH ENGINE %s TO %s;", ENGINE_NAME, DATABASE_NAME),
					String.format("ALTER DATABASE %s WITH DESCRIPTION = 'JDBC Integration test'", DATABASE_NAME),
					String.format("ALTER ENGINE %s RENAME TO %s", ENGINE_NAME, ENGINE_NEW_NAME),
					String.format("START ENGINE %s", ENGINE_NEW_NAME), String.format("STOP ENGINE %s", ENGINE_NEW_NAME),
					String.format("DROP ENGINE %s", ENGINE_NEW_NAME), String.format("DROP DATABASE %s", DATABASE_NAME));
			for (String query : queries) {
				try (Statement statement = connection.createStatement()) {
					statement.execute(query);
				}
			}
		}
	}
}
