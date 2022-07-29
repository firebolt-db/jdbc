package integration.tests;

import static com.firebolt.jdbc.exception.ExceptionType.REQUEST_FAILED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.*;

import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.statement.FireboltStatement;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CancelTest extends IntegrationTest {

	@BeforeAll
	void beforeAll() {
		executeStatementFromFile("/queries/cancel/cancel-test-ddl.sql");
	}

	@AfterAll
	void afterAll() {
		executeStatementFromFile("/queries/cancel/clean-up.sql");
	}

	@Test
	@Timeout(value = 2, unit = TimeUnit.MINUTES)
	void shouldCancelQuery() throws SQLException, InterruptedException {
		String tableName = extractTableName();
		long totalRecordsToInsert = 1000000000L;
		String query = String.format("INSERT INTO %s SELECT id FROM generateRandom('id Int8') LIMIT %d", tableName,
				totalRecordsToInsert);
		try (Connection connection = createConnection()) {
			try (FireboltStatement insertStatement = (FireboltStatement) connection.createStatement()) {
				this.setParam(connection, "use_standard_sql", "0");
				Thread thread = new Thread(() -> {
					try {
						insertStatement.execute(query);
					} catch (FireboltException e) {
						if (!e.getType().equals(REQUEST_FAILED)) {
							throw new RuntimeException(e);
							// REQUEST_FAILED is expected since the query was aborted
						}
					} catch (SQLException e) {
						throw new RuntimeException(e);
					}
				});
				thread.start();
				while (!insertStatement.isStatementRunning() && thread.isAlive()) {
					Thread.sleep(1000);
				}
				insertStatement.cancel();
			}
			Thread.sleep(5000);
			verifyThatNoMoreRecordsAreAdded(connection, tableName, totalRecordsToInsert);
		}
	}

	private void verifyThatNoMoreRecordsAreAdded(Connection connection, String tableName, long totalRecordsToInsert)
			throws SQLException, InterruptedException {
		String countAddedRecordsQuery = String.format("SELECT COUNT(*) FROM %s", tableName);
		try (Statement countStatement = connection.createStatement()) {
			ResultSet rs = countStatement.executeQuery(countAddedRecordsQuery);
			rs.next();
			long count = rs.getInt(1);
			log.info("{} records were added to table {} before the statement got cancelled", count, tableName);
			Thread.sleep(5000); // waiting to see if more records are being added
			rs = countStatement.executeQuery(countAddedRecordsQuery);
			rs.next();
			assertEquals(count, rs.getInt(1));
			assertTrue(count < totalRecordsToInsert, "No new records were added following the cancellation");
			rs.close();
		}

	}

	private String extractTableName() throws SQLException {
		String tableName = null;
		try (Connection connection = this.createConnection(); Statement statement = connection.createStatement()) {
			this.setParam(connection, "use_standard_sql", "0");
			ResultSet resultSet = statement.executeQuery("SHOW tables");
			while (resultSet.next() && tableName == null) {
				log.info(resultSet.getString(1));
				if (StringUtils.startsWith(resultSet.getString(1), "cancel_test")
						&& StringUtils.endsWith(resultSet.getString(1), "_distributed")) {
					tableName = resultSet.getString(1);
				}

			}
		}
		return tableName;
	}

}
