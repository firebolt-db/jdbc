package integration.tests;

import static java.sql.Statement.SUCCESS_NO_INFO;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.firebolt.jdbc.QueryResult;
import com.firebolt.jdbc.resultset.FireboltResultSet;
import com.firebolt.jdbc.testutils.AssertionUtil;
import com.firebolt.jdbc.type.FireboltDataType;

import integration.ConnectionInfo;
import integration.IntegrationTest;
import lombok.Builder;
import lombok.CustomLog;
import lombok.Value;

@CustomLog
class PreparedStatementTest extends IntegrationTest {

	@BeforeEach
	void beforeEach() {
		executeStatementFromFile("/statements/prepared-statement/ddl.sql");
	}

	@AfterEach
	void afterEach() {
		executeStatementFromFile("/statements/prepared-statement/cleanup.sql");
	}

	@Test
	void shouldInsertRecordsInBatch() throws SQLException {
		Car car1 = Car.builder().make("Ford").sales(150).build();
		Car car2 = Car.builder().make("Tesla").sales(300).build();
		try (Connection connection = createConnection()) {

			try (PreparedStatement statement = connection
					.prepareStatement("INSERT INTO prepared_statement_test (sales, make) VALUES (?,?)")) {
				statement.setObject(1, car1.getSales());
				statement.setObject(2, car1.getMake());
				statement.addBatch();
				statement.setObject(1, car2.getSales());
				statement.setObject(2, car2.getMake());
				statement.addBatch();
				int[] result = statement.executeBatch();
				assertArrayEquals(new int[] { SUCCESS_NO_INFO, SUCCESS_NO_INFO }, result);
			}

			List<List<?>> expectedRows = new ArrayList<>();
			expectedRows.add(Arrays.asList(car1.getSales(), car1.getMake()));
			expectedRows.add(Arrays.asList(car2.getSales(), car2.getMake()));

			QueryResult queryResult = this.createExpectedResult(expectedRows);

			try (Statement statement = connection.createStatement();
					ResultSet rs = statement
							.executeQuery("SELECT sales, make FROM prepared_statement_test ORDER BY make");
					ResultSet expectedRs = FireboltResultSet.of(queryResult)) {
				AssertionUtil.assertResultSetEquality(expectedRs, rs);
			}
		}
	}

	@Test
	void shouldReplaceParamMarkers() throws SQLException {
		String insertSql = "INSERT INTO prepared_statement_test(sales, make) VALUES /* Some comment ? */ -- other comment ? \n  (?,?)";
		try (Connection connection = createConnection()) {

			try (PreparedStatement statement = connection.prepareStatement(insertSql)) {
				statement.setObject(1, 200);
				statement.setObject(2, "VW");
				assertFalse(statement.execute());
			}

			List<List<?>> expectedRows = new ArrayList<>();
			expectedRows.add(Arrays.asList(200, "VW"));

			String selectSql = "SELECT sales, make FROM prepared_statement_test WHERE make = ?";
			try (PreparedStatement selectStatement = connection.prepareStatement(selectSql)) {
				QueryResult expectedResult = this.createExpectedResult(expectedRows);
				selectStatement.setString(1, "VW");
				try (ResultSet rs = selectStatement.executeQuery();
						ResultSet expectedRs = FireboltResultSet.of(expectedResult)) {
					AssertionUtil.assertResultSetEquality(expectedRs, rs);

				}

			}
		}
	}

	@Test
	void shouldParReplaceParamMarkersInMultistatementStatement() throws SQLException {
		String insertSql = "INSERT INTO prepared_statement_test(sales, make) VALUES /* Some comment ? */ -- other comment ? \n  (?,?);"
				+ "INSERT INTO prepared_statement_test(sales, make) VALUES (?,?)";
		try (Connection connection = createConnection()) {

			try (PreparedStatement statement = connection.prepareStatement(insertSql)) {
				statement.setObject(1, 5000);
				statement.setObject(2, "Porsche");
				statement.setObject(3, 10000);
				statement.setObject(4, "Ferrari");
				assertFalse(statement.execute());
			}

			List<List<?>> firstExpectedRows = new ArrayList<>();
			firstExpectedRows.add(Arrays.asList(5000, "Porsche"));
			List<List<?>> secondExceptedRows = new ArrayList<>();
			secondExceptedRows.add(Arrays.asList(10000, "Ferrari"));

			String selectSql = "SELECT sales, make FROM prepared_statement_test WHERE make = ?; SELECT sales, make FROM prepared_statement_test WHERE make = ?";
			try (PreparedStatement selectStatement = connection.prepareStatement(selectSql)) {
				QueryResult expectedFirstResult = createExpectedResult(firstExpectedRows);
				QueryResult expectedSecondResult = createExpectedResult(secondExceptedRows);
				selectStatement.setString(1, "Porsche");
				selectStatement.setString(2, "Ferrari");
				selectStatement.execute();
				try (ResultSet rs = selectStatement.getResultSet();
						ResultSet expectedRs = FireboltResultSet.of(expectedFirstResult)) {
					AssertionUtil.assertResultSetEquality(expectedRs, rs);
				}
				assertTrue(selectStatement.getMoreResults());
				try (ResultSet rs = selectStatement.getResultSet();
						ResultSet expectedRs = FireboltResultSet.of(expectedSecondResult)) {
					AssertionUtil.assertResultSetEquality(expectedRs, rs);
				}

			}
		}
	}

	@Test
	void shouldFailSQLInjectionAttempt() throws SQLException {
		String insertSql = "INSERT INTO prepared_statement_test(sales, make) VALUES /* Some comment ? */ -- other comment ? \n  (?,?)";
		try (Connection connection = createConnection()) {

			try (PreparedStatement statement = connection.prepareStatement(insertSql)) {
				statement.setObject(1, 200);
				statement.setObject(2, "VW");
				assertFalse(statement.execute());
			}

			String selectSql = "SELECT sales, make FROM prepared_statement_test WHERE make = ?";
			try (PreparedStatement statement = connection.prepareStatement(selectSql)) {
				QueryResult emptyResult = createExpectedResult(Collections.emptyList());
				statement.setString(1, "VW' OR 1=1");
				try (ResultSet rs = statement.executeQuery();
						ResultSet expectedRs = FireboltResultSet.of(emptyResult)) {
					AssertionUtil.assertResultSetEquality(expectedRs, rs);
				}

			}
		}
	}

	private QueryResult createExpectedResult(List<List<?>> expectedRows) {
		return QueryResult.builder().databaseName(ConnectionInfo.getInstance().getDatabase())
				.tableName("prepared_statement_test")
				.columns(Arrays.asList(QueryResult.Column.builder().name("sales").type(FireboltDataType.INT_64).build(),
						QueryResult.Column.builder().name("make").type(FireboltDataType.STRING).build()))
				.rows(expectedRows).build();

	}

	@Builder
	@Value
	private static class Car {
		Integer sales;
		String make;
	}

}
