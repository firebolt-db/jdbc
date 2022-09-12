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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.statement.FireboltStatement;

import integration.IntegrationTest;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class StatementCancelTest extends IntegrationTest {

	@BeforeEach
	void beforeEach() {
		executeStatementFromFile("/statements/statement-cancel/ddl.sql");
	}

	@AfterEach
	void afterEach() {
		executeStatementFromFile("/statements/statement-cancel/cleanup.sql");
	}

	@Test
	@Timeout(value = 2, unit = TimeUnit.MINUTES)
	void shouldCancelQuery() throws SQLException, InterruptedException {
		try (Connection connection = createConnection()) {
			this.setParam(connection, "use_standard_sql", "0");
			String tableName = extractTableNameWithNonStandardSql(connection, "first_statement_cancel_test");
			String secondTableName = extractTableNameWithNonStandardSql(connection, "second_statement_cancel_test");
			long totalRecordsToInsert = 1000000000L;
			String query = String.format("INSERT INTO %s SELECT id FROM generateRandom('id Int8') LIMIT %d; INSERT INTO %s(id) values(1) ", tableName,
					totalRecordsToInsert, secondTableName);

			try (FireboltStatement insertStatement = (FireboltStatement) connection.createStatement()) {

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
				while (!insertStatement.isStatementRunning()) {
					Thread.sleep(1000);
				}
				insertStatement.cancel();
			}
			Thread.sleep(5000);
			verifyThatNoMoreRecordsAreAdded(connection, tableName, totalRecordsToInsert);
			verifyThatSecondStatementWasNotExecuted(connection, secondTableName);

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

	private void verifyThatSecondStatementWasNotExecuted(Connection connection, String tableName) throws SQLException {
		String countAddedRecordsQuery = String.format("SELECT COUNT(*) FROM %s", tableName);
		try (Statement countStatement = connection.createStatement()) {
			ResultSet rs = countStatement.executeQuery(countAddedRecordsQuery);
			rs.next();
			assertEquals(0, rs.getInt(1));
		}
	}
	/**
	 * Extract table name when non-standard sql is used
	 */
	private String extractTableNameWithNonStandardSql(Connection connection, String testTableName) throws SQLException {
		String tableName = null;
		try (Statement statement = connection.createStatement()) {
			ResultSet resultSet = statement.executeQuery("SHOW tables");
			while (resultSet.next() && tableName == null) {
				log.info(resultSet.getString(1));
				if (StringUtils.startsWith(resultSet.getString(1), testTableName)
						&& StringUtils.endsWith(resultSet.getString(1), "_distributed")) {
					tableName = resultSet.getString(1);
				}

			}
		}
		return tableName;
	}

}
