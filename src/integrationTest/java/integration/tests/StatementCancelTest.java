package integration.tests;

import com.firebolt.jdbc.exception.ExceptionType;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.statement.FireboltStatement;
import com.firebolt.jdbc.testutils.TestTag;
import integration.EnvironmentCondition;
import integration.IntegrationTest;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static integration.EnvironmentCondition.Attribute.databaseVersion;
import static integration.EnvironmentCondition.Comparison.GE;
import static org.junit.jupiter.api.Assertions.assertEquals;

@CustomLog
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
	@Tag(TestTag.V1) // generate_series is supported on all available engine of v2
	@Tag(TestTag.SLOW)
	void shouldCancelQueryV1() throws SQLException, InterruptedException {
		shouldCancelQuery();
	}

	@Test
	@Timeout(value = 10, unit = TimeUnit.MINUTES)
	@EnvironmentCondition(value = "3.33", attribute = databaseVersion, comparison = GE) // generate_series is supported starting from version 3.33 on v2
	@Tag(TestTag.V2)
	@Tag(TestTag.SLOW)
	void shouldCancelQueryV2() throws SQLException, InterruptedException {

		shouldCancelQuery();
	}

	private void shouldCancelQuery() throws SQLException, InterruptedException {
		try (Connection connection = createConnection(); Statement fillStatement = connection.createStatement()) {
			long now = System.currentTimeMillis();
			fillStatement.execute("insert into ex_lineitem ( l_orderkey ) SELECT * FROM GENERATE_SERIES(1, 100000000)");
			long insertTime = System.currentTimeMillis() - now;
			log.info("Insert time took: {} millis", insertTime);

			try (Statement insertStatement = connection.createStatement()) {

				Thread thread = new Thread(() -> {
					try {
						insertStatement.execute("INSERT INTO first_statement_cancel_test SELECT * FROM ex_lineitem; INSERT INTO second_statement_cancel_test SELECT * FROM ex_lineitem;");
					} catch (FireboltException e) {
						if (!e.getType().equals(ExceptionType.CANCELED)) {
							throw new RuntimeException(e);
							// CANCELED is expected since the query was aborted
						}
					} catch (SQLException e) {
						throw new RuntimeException(e);
					}
				});
				thread.start();
				// Wait until copying started
				while (!((FireboltStatement)insertStatement).isStatementRunning()) {
					Thread.sleep(100);
				}
				Thread.sleep(insertTime / 5); // wait 20% of time that was spent to fill data to give chance to the insert statement to copy data

				log.info("Cancelling the statement");
				insertStatement.cancel(); // now cancel the statement
			}
			verifyThatNoMoreRecordsAreAdded(connection, "first_statement_cancel_test", insertTime);
			verifyThatSecondStatementWasNotExecuted(connection, "second_statement_cancel_test");
		}
	}

	private void verifyThatNoMoreRecordsAreAdded(Connection connection, String tableName, long insertTime) throws SQLException, InterruptedException {
		// Get number of rows in the table. Do it several times until we get something. Wait for 10% of time that spent to fill the table.
		// We need several attempts because this DB does not support transactions, so sometimes it takes time until the
		// data is available.
		long waitForResultTime = insertTime / 2;
		long waitForResultDelay = waitForResultTime / 10;
		log.info("verifyThatNoMoreRecordsAreAdded insertTime={}, waitForResultTime={}", insertTime, waitForResultTime);
		int count0;
		int i = 0;
		for (count0 = count(connection, tableName); i < 10; count0 = count(connection, tableName), i++) {
			log.info("verifyThatNoMoreRecordsAreAdded count0={}", count0);
			if (count0 > 0) {
				break;
			}
			Thread.sleep(waitForResultDelay);
		}

		// Wait for more time that we spent to fill the table.
		// We want to wait enough to give a chance to the query to fill more data.
		Thread.sleep(insertTime); // waiting to see if more records are being added
		int count1 = count(connection, tableName);
		Thread.sleep(insertTime); // waiting to see if more records are being added
		int count2 = count(connection, tableName);
		log.info("verifyThatNoMoreRecordsAreAdded count1={}, count2={}", count1, count2);
		assertEquals(count1, count2);
	}

	private int count(Connection connection, String tableName) throws SQLException {
		String countAddedRecordsQuery = String.format("SELECT COUNT(*) FROM %s", tableName);
		try (Statement countStatement = connection.createStatement(); ResultSet rs = countStatement.executeQuery(countAddedRecordsQuery)) {
			return rs.next() ? rs.getInt(1) : 0;
		}
	}

	private void verifyThatSecondStatementWasNotExecuted(Connection connection, String tableName) throws SQLException {
		assertEquals(0, count(connection, tableName));
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
				String rsTableName = resultSet.getString(1);
				if (rsTableName != null && rsTableName.startsWith(testTableName) && rsTableName.endsWith("_distributed")) {
					tableName = resultSet.getString(1);
				}
			}
		}
		return tableName;
	}

}
