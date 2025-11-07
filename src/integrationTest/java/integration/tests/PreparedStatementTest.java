package integration.tests;

import com.firebolt.jdbc.CheckedBiFunction;
import com.firebolt.jdbc.CheckedVoidTriFunction;
import com.firebolt.jdbc.QueryResult;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.resultset.FireboltResultSet;
import com.firebolt.jdbc.testutils.AssertionUtil;
import com.firebolt.jdbc.testutils.TestTag;
import com.firebolt.jdbc.type.FireboltDataType;
import integration.ConnectionInfo;
import integration.IntegrationTest;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Builder;
import lombok.CustomLog;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junitpioneer.jupiter.DefaultTimeZone;

import static java.sql.Statement.SUCCESS_NO_INFO;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
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

			QueryResult queryResult = createExpectedResult(expectedRows);

			try (Statement statement = connection.createStatement();
				 ResultSet rs = statement
						 .executeQuery("SELECT sales, make FROM prepared_statement_test ORDER BY make");
				 ResultSet expectedRs = FireboltResultSet.of(queryResult)) {
				AssertionUtil.assertResultSetEquality(expectedRs, rs);
			}
		}
	}

	Stream<Arguments> numericTypes() {
		return Stream.of(
				Arguments.of("byte",
						(CheckedVoidTriFunction<PreparedStatement, Integer, Integer>) (s, i, v) -> {
							s.setByte(i, v.byteValue());
						}, (CheckedBiFunction<ResultSet, Integer, Number>) (rs, i) -> (int) rs.getByte(i)),

				Arguments.of("short",
						(CheckedVoidTriFunction<PreparedStatement, Integer, Integer>) (s, i, v) -> {
							s.setShort(i, v.shortValue());
						}, (CheckedBiFunction<ResultSet, Integer, Number>) (rs, i) -> (int) rs.getShort(i)),

				Arguments.of("int",
						(CheckedVoidTriFunction<PreparedStatement, Integer, Integer>) PreparedStatement::setInt,
						(CheckedBiFunction<ResultSet, Integer, Number>) (rs, i) -> (int) rs.getInt(i)),

				Arguments.of("long",
						(CheckedVoidTriFunction<PreparedStatement, Integer, Integer>) (s, i, v) -> {
							s.setLong(i, v.longValue());
						}, (CheckedBiFunction<ResultSet, Integer, Number>) (rs, i) -> (int) rs.getLong(i)),

				Arguments.of("getObject(Long.class)",
						(CheckedVoidTriFunction<PreparedStatement, Integer, Integer>) (s, i, v) -> {
							s.setLong(i, v.longValue());
						}, (CheckedBiFunction<ResultSet, Integer, Number>) (rs, i) -> rs.getObject(i, Long.class).intValue()),

				Arguments.of("getObject(i, java.util.Map.of(\"long\", Integer.class)",
						(CheckedVoidTriFunction<PreparedStatement, Integer, Integer>) (s, i, v) -> {
							s.setLong(i, v.longValue());
						}, (CheckedBiFunction<ResultSet, Integer, Number>) (rs, i) -> (int) rs.getObject(i, java.util.Map.of("long", Integer.class)))
		);
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("numericTypes")
	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	<T> void shouldInsertRecordsUsingDifferentNumericTypes(String name, CheckedVoidTriFunction<PreparedStatement, Integer, Integer> setter, CheckedBiFunction<ResultSet, Integer, T> getter) throws SQLException {
		Car car = Car.builder().make("Tesla").sales(42).build();
		try (Connection connection = createConnection()) {

			try (PreparedStatement statement = connection
					.prepareStatement("INSERT INTO prepared_statement_test (sales, make) VALUES (?,?)")) {
				setter.apply(statement, 1, car.getSales());
				statement.setString(2, car.getMake());
				statement.executeUpdate();
			}

			try (Statement statement = connection.createStatement();
				 ResultSet rs = statement.executeQuery("SELECT sales, make FROM prepared_statement_test ORDER BY make")) {
				assertTrue(rs.next());
				assertEquals(car.getSales(), getter.apply(rs, 1));
				assertEquals(car.getMake(), rs.getString(2));
				rs.getString(2);

				assertFalse(rs.next());
			}
		}
	}

	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
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
				QueryResult expectedResult = createExpectedResult(expectedRows);
				selectStatement.setString(1, "VW");
				try (ResultSet rs = selectStatement.executeQuery();
						ResultSet expectedRs = FireboltResultSet.of(expectedResult)) {
					AssertionUtil.assertResultSetEquality(expectedRs, rs);

				}

			}
		}
	}

	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
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

	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
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

	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	@Test
	void shouldInsertAndSelectByteArray() throws SQLException {
		Car car1 = Car.builder().make("Ford").sales(12345).signature("Henry Ford".getBytes()).build();
		Car car2 = Car.builder().make("Tesla").sales(54321).signature("Elon Musk".getBytes()).build();
		try (Connection connection = createConnection()) {

			try (PreparedStatement statement = connection
					.prepareStatement("INSERT INTO prepared_statement_test (sales, make, signature) VALUES (?,?,?)")) {
				statement.setLong(1, car1.getSales());
				statement.setString(2, car1.getMake());
				statement.setBytes(3, car1.getSignature());
				statement.addBatch();
				statement.setLong(1, car2.getSales());
				statement.setString(2, car2.getMake());
				statement.setBytes(3, car2.getSignature());
				statement.addBatch();
				int[] result = statement.executeBatch();
				assertArrayEquals(new int[] { SUCCESS_NO_INFO, SUCCESS_NO_INFO }, result);
			}

			Set<Car> actual = new HashSet<>();
			try (Statement statement = connection.createStatement();
				 ResultSet rs = statement.executeQuery("SELECT sales, make, signature FROM prepared_statement_test")) {
				while(rs.next()) {
					actual.add(Car.builder().sales(rs.getInt(1)).make(rs.getString(2)).signature(rs.getBytes(3)).build());
				}
			}
			assertEquals(Set.of(car1, car2), actual);
		}
	}

	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	@Test
	void shouldInsertAndSelectBlobClob() throws SQLException, IOException {
		Car car1 = Car.builder().make("Ford").sales(12345).signature("Henry Ford".getBytes()).build();
		Car car2 = Car.builder().make("Tesla").sales(54321).signature("Elon Musk".getBytes()).build();
		try (Connection connection = createConnection()) {

			try (PreparedStatement statement = connection
					.prepareStatement("INSERT INTO prepared_statement_test (sales, make, signature) VALUES (?,?,?)")) {
				statement.setLong(1, car1.getSales());
				statement.setClob(2, clob(connection, car1.getMake()));
				statement.setBlob(3, blob(connection, car1.getSignature()));
				statement.addBatch();
				statement.setLong(1, car2.getSales());
				statement.setClob(2, clob(connection, car2.getMake()));
				statement.setBlob(3, blob(connection, car2.getSignature()));
				statement.addBatch();
				int[] result = statement.executeBatch();
				assertArrayEquals(new int[] { SUCCESS_NO_INFO, SUCCESS_NO_INFO }, result);
			}

			Set<Car> actual = new HashSet<>();
			try (Statement statement = connection.createStatement();
				 ResultSet rs = statement.executeQuery("SELECT sales, make, signature FROM prepared_statement_test")) {
				while(rs.next()) {
					actual.add(Car.builder()
							.sales(rs.getInt(1))
							.make(new String(new BufferedReader(rs.getClob(2).getCharacterStream()).lines().collect(Collectors.joining(System.lineSeparator()))))
							.signature(rs.getBlob(3).getBinaryStream().readAllBytes())
							.build());
				}
			}
			assertEquals(Set.of(car1, car2), actual);
		}
	}

	private Blob blob(Connection connection, byte[] bytes) throws SQLException {
		Blob blob = connection.createBlob();
		blob.setBytes(1, bytes);
		return blob;
	}

	private Clob clob(Connection connection, String text) throws SQLException {
		Clob clob = connection.createClob();
		clob.setString(1, text);
		return clob;
	}

	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	@Test
	void shouldInsertAndSelectStreams() throws SQLException, IOException {
		Car car1 = Car.builder().make("Ford").sales(12345).signature("Henry Ford".getBytes()).build();
		Car car2 = Car.builder().make("Tesla").sales(54321).signature("Elon Musk".getBytes()).build();
		try (Connection connection = createConnection()) {

			try (PreparedStatement statement = connection
					.prepareStatement("INSERT INTO prepared_statement_test (sales, make, signature) VALUES (?,?,?)")) {
				statement.setLong(1, car1.getSales());
				statement.setCharacterStream(2, new StringReader(car1.getMake()));
				statement.setBinaryStream(3, new ByteArrayInputStream(car1.getSignature()));
				statement.addBatch();
				statement.setLong(1, car2.getSales());
				statement.setCharacterStream(2, new StringReader(car2.getMake()));
				statement.setBinaryStream(3, new ByteArrayInputStream(car2.getSignature()));
				statement.addBatch();
				int[] result = statement.executeBatch();
				assertArrayEquals(new int[] { SUCCESS_NO_INFO, SUCCESS_NO_INFO }, result);
			}

			Set<Car> actual = new HashSet<>();
			try (Statement statement = connection.createStatement();
				 ResultSet rs = statement.executeQuery("SELECT sales, make, signature FROM prepared_statement_test")) {
				while(rs.next()) {
					actual.add(Car.builder()
							.sales(rs.getInt(1))
							.make(new String(rs.getAsciiStream(2).readAllBytes()))
							.signature(rs.getBinaryStream(3).readAllBytes())
							.build());
				}
			}
			assertEquals(Set.of(car1, car2), actual);
		}
	}

	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	@Test
	void shouldInsertAndSelectDateTime() throws SQLException {
		Car car1 = Car.builder().make("Ford").sales(12345).ts(new Timestamp(2)).d(new Date(3)).build();
		try (Connection connection = createConnection()) {

			try (PreparedStatement statement = connection
					.prepareStatement("INSERT INTO prepared_statement_test (sales, make, ts, d) VALUES (?,?,?,?)")) {
				statement.setLong(1, car1.getSales());
				statement.setString(2, car1.getMake());
				statement.setTimestamp(3, car1.getTs());
				statement.setDate(4, car1.getD());
				statement.executeUpdate();
			}

			try (Statement statement = connection.createStatement();
				 ResultSet rs = statement.executeQuery("SELECT sales, make, ts, d FROM prepared_statement_test")) {
				assertTrue(rs.next());
				Car actual = Car.builder().sales(rs.getInt(1)).make(rs.getString(2)).ts(rs.getTimestamp(3)).d(rs.getDate(4)).build();
				assertFalse(rs.next());
				// Date type in DB does not really hold the time, so the time part is unpredictable and cannot be compared.
				// This is the reason to compare string representation of the object: Date.toString() returns the date only
				// without hours, minutes and seconds.
				assertEquals(car1.toString(), actual.toString());
			}
		}
	}


	@Test
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	void shouldInsertAndSelectGeography() throws SQLException {

		executeStatementFromFile("/statements/prepared-statement/ddl_2_0.sql");
		try (Connection connection = createConnection()) {

			try (PreparedStatement statement = connection
					.prepareStatement("INSERT INTO prepared_statement_2_0_test (make, location) VALUES (?,?)")) {
				statement.setString(1, "Ford");
				statement.setString(2, "POINT(1 1)");
				statement.executeUpdate();
			}

			try (Statement statement = connection.createStatement();
					ResultSet rs = statement
							.executeQuery("SELECT location FROM prepared_statement_2_0_test")) {
				rs.next();
				assertEquals(FireboltDataType.GEOGRAPHY.name().toLowerCase(),
						rs.getMetaData().getColumnTypeName(1).toLowerCase());
				assertEquals("0101000020E6100000FEFFFFFFFFFFEF3F000000000000F03F", rs.getString(1));
			}
		} finally {
			executeStatementFromFile("/statements/prepared-statement/cleanup_2_0.sql");
		}
	}

	@Test
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	void shouldInsertAndSelectStruct() throws SQLException {
		Car car1 = Car.builder().make("Ford").sales(12345).ts(new Timestamp(2)).d(new Date(3)).build();

		try (Connection connection = createConnection()) {

			try (PreparedStatement statement = connection
					.prepareStatement("INSERT INTO prepared_statement_test (sales, make, ts, d) VALUES (?,?,?,?)")) {
				statement.setLong(1, car1.getSales());
				statement.setString(2, car1.getMake());
				statement.setTimestamp(3, car1.getTs());
				statement.setDate(4, car1.getD());
				statement.executeUpdate();
			}
			setParam(connection, "advanced_mode", "true");
			setParam(connection, "enable_struct_syntax", "true");
			try (Statement statement = connection.createStatement();
					ResultSet rs = statement
							.executeQuery("SELECT prepared_statement_test FROM prepared_statement_test")) {
				rs.next();
				assertEquals(FireboltDataType.STRUCT.name().toLowerCase()
								+ "(make text, sales long, ts timestamp null, d date null, signature bytea null, url text null, tsz timestamptz null)",
						rs.getMetaData().getColumnTypeName(1).toLowerCase());
				String expectedJson = String.format("{\"make\":\"%s\",\"sales\":\"%d\",\"ts\":\"%s\",\"d\":\"%s\",\"signature\":null,\"url\":null,\"tsz\":null}",
						car1.getMake(), car1.getSales(), car1.getTs().toString(), car1.getD().toString());
				assertEquals(expectedJson, rs.getString(1));
			}
		} finally {
			executeStatementFromFile("/statements/prepared-statement/cleanup.sql");
		}
	}

	@ParameterizedTest
	@DefaultTimeZone("UTC")
	@MethodSource("dateTypes")
	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	void shouldFetchTimestampAndDate(Object timestampOrLocalDateTime, Object dateOrLocalDate, boolean addTargetSqlType) throws SQLException {
		String expectedTimestamp = "2019-07-31 11:15:13";
		String expectedDate = "2019-07-31";
		try (Connection connection = createConnection()) {
			try (PreparedStatement statement = connection
					.prepareStatement("INSERT INTO prepared_statement_test (ts, d, make, sales) VALUES (?,?, '', 0)")) {
				// we need to specify make and sales values since they are not null
				if (addTargetSqlType) {
					statement.setObject(1, timestampOrLocalDateTime, Types.TIMESTAMP);
					statement.setObject(2, dateOrLocalDate, Types.DATE);
				} else {
					statement.setObject(1, timestampOrLocalDateTime);
					statement.setObject(2, dateOrLocalDate);
				}
				statement.executeUpdate();
			}
			try (Statement statement = connection.createStatement();
				 ResultSet rs = statement
						 .executeQuery("SELECT ts, d FROM prepared_statement_test")) {
				rs.next();
				assertEquals(expectedTimestamp, rs.getString(1));
				assertEquals(expectedDate, rs.getString(2));
			}
		} finally {
			executeStatementFromFile("/statements/prepared-statement/cleanup.sql");
		}
	}

	@Test
	@DefaultTimeZone("UTC")
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	void shouldFetchTimestampWithTimezone() throws SQLException {
		String expectedUtcTimestamp = "2019-07-31 10:15:13.123456+00";

		// will use this local date "2019-07-31 11:15:13" for Europe/Berlin timezone
		// also firebolt only keeps microseconds and offset date time supports nanoseconds so in order to set: 123456 microseconds we need to use 123456000
		OffsetDateTime timestampWithTimezone = OffsetDateTime.of(
				2019, 7, 31,     // year, month, day
				11, 15, 13, 123456000,    // hour, minute, second, nanosecond
				ZoneOffset.of("+01:00")   // for Europe/Berlin
		);

		try (Connection connection = createConnection()) {
			try (PreparedStatement statement = connection
					.prepareStatement("INSERT INTO prepared_statement_test (ts, d, make, sales, tsz) VALUES (NULL,NULL, '', 0,?)")) {
				statement.setObject(1, timestampWithTimezone);
				statement.executeUpdate();
			}

			try (Statement statement = connection.createStatement();
				 ResultSet rs = statement
						 .executeQuery("SELECT tsz FROM prepared_statement_test")) {
				rs.next();
				OffsetDateTime offsetDateTime = rs.getObject(1, OffsetDateTime.class);
				assertEquals(timestampWithTimezone.toInstant(), offsetDateTime.toInstant()); // they should represent the same instant in time

				assertEquals(expectedUtcTimestamp, rs.getString(1)); // string value will be in UTC timezone
			}
		} finally {
			executeStatementFromFile("/statements/prepared-statement/cleanup.sql");
		}
	}

	@Test
	@DefaultTimeZone("UTC")
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	void canUseTimestampParameterWithSqlType() throws SQLException {
		String expectedUtcTimestamp = "2019-07-31 10:15:13.123456+00";

		// will use this local date "2019-07-31 11:15:13" for Europe/Berlin timezone
		// also firebolt only keeps microseconds and offset date time supports nanoseconds so in order to set: 123456 microseconds we need to use 123456000
		OffsetDateTime timestampWithTimezone = OffsetDateTime.of(
				2019, 7, 31,     // year, month, day
				11, 15, 13, 123456000,    // hour, minute, second, nanosecond
				ZoneOffset.of("+01:00")   // for Europe/Berlin
		);

		try (Connection connection = createConnection()) {
			try (PreparedStatement statement = connection
					.prepareStatement("INSERT INTO prepared_statement_test (ts, d, make, sales, tsz) VALUES (NULL,NULL, '', 0,?)")) {
				statement.setObject(1, timestampWithTimezone, java.sql.Types.TIMESTAMP_WITH_TIMEZONE);
				statement.executeUpdate();
			}

			try (Statement statement = connection.createStatement();
				 ResultSet rs = statement
						 .executeQuery("SELECT tsz FROM prepared_statement_test")) {
				rs.next();
				OffsetDateTime offsetDateTime = rs.getObject(1, OffsetDateTime.class);
				assertEquals(timestampWithTimezone.toInstant(), offsetDateTime.toInstant()); // they should represent the same instant in time

				assertEquals(expectedUtcTimestamp, rs.getString(1)); // string value will be in UTC timezone
			}
		} finally {
			executeStatementFromFile("/statements/prepared-statement/cleanup.sql");
		}
	}

	@Test
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	void shouldInsertAndSelectComplexStruct() throws SQLException {
		Car car1 = Car.builder().ts(new Timestamp(2)).d(new Date(3)).tags(new String[] { "fast", "sleek" }).build();

		executeStatementFromFile("/statements/prepared-statement-struct/ddl.sql");
		try (Connection connection = createConnection()) {

			try (PreparedStatement statement = connection
					.prepareStatement("INSERT INTO test_struct_helper(a, \"b column\") VALUES (?,?)")) {
				statement.setArray(1, connection.createArrayOf("VARCHAR", car1.getTags()));
				statement.setTimestamp(2, car1.getTs());
				statement.executeUpdate();
			}

			setParam(connection, "advanced_mode", "true");
			setParam(connection, "enable_struct_syntax", "true");
			try (Statement statement = connection.createStatement()) {
				statement.execute(
						"INSERT INTO test_struct(id, s) SELECT 1, test_struct_helper FROM test_struct_helper");
			}
			try (Statement statement = connection.createStatement();
					ResultSet rs = statement
							.executeQuery("SELECT test_struct FROM test_struct")) {
				rs.next();
				assertEquals(FireboltDataType.STRUCT.name().toLowerCase()
						+ "(id int, s struct(a array(text null) null, `b column` timestamp null))",
						rs.getMetaData().getColumnTypeName(1).toLowerCase());
				String expectedJson = String.format(
						"{\"id\":%d,\"s\":{\"a\":[\"%s\",\"%s\"],\"b column\":\"%s\"}}", 1, car1.getTags()[0],
								car1.getTags()[1], car1.getTs().toString());
				assertEquals(expectedJson, rs.getString(1));
			}
		} finally {
			executeStatementFromFile("/statements/prepared-statement-struct/cleanup.sql");
		}
	}

	private QueryResult createExpectedResult(List<List<?>> expectedRows) {
		return QueryResult.builder().databaseName(ConnectionInfo.getInstance().getDatabase())
				.tableName("prepared_statement_test")
				.columns(
						Arrays.asList(QueryResult.Column.builder().name("sales").type(FireboltDataType.BIG_INT).build(),
								QueryResult.Column.builder().name("make").type(FireboltDataType.TEXT).build()))
				.rows(expectedRows).build();

	}

	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	@Test
	void shouldInsertAndRetrieveUrl() throws SQLException, MalformedURLException {
		Car tesla = Car.builder().make("Tesla").url(new URL("https://www.tesla.com/")).sales(300).build();
		Car nothing = Car.builder().sales(0).build();
		try (Connection connection = createConnection()) {
			try (PreparedStatement statement = connection
					.prepareStatement("INSERT INTO prepared_statement_test (sales, make, url) VALUES (?,?,?)")) {
				statement.setInt(1, tesla.getSales());
				statement.setString(2, tesla.getMake());
				statement.setURL(3, tesla.getUrl());
				statement.executeUpdate();
				statement.setInt(1, nothing.getSales());
				statement.setString(2, "");
				statement.setURL(3, null);
				statement.executeUpdate();
			}

			try (Statement statement = connection.createStatement();
				 ResultSet rs = statement.executeQuery("SELECT url FROM prepared_statement_test order by sales")) {
				assertTrue(rs.next());
				assertNull(rs.getString(1));
				assertNull(rs.getURL(1));
				assertTrue(rs.next());
				assertEquals("https://www.tesla.com/", rs.getString(1));
				assertEquals(new URL("https://www.tesla.com/"), rs.getURL(1));
				assertFalse(rs.next());
			}
		}
	}

	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	@Test
	void shouldFetchSpecialCharacters() throws SQLException, MalformedURLException {
		try (Connection connection = createConnection()) {
			try (PreparedStatement statement = connection
					.prepareStatement("SELECT ? as a, ? as b, ? as c, ? as d")) {
				statement.setString(1, "ї");
				statement.setString(2, "\n");
				statement.setString(3, "\\");
				statement.setString(4, "don't");
				statement.execute();
				ResultSet rs = statement.getResultSet();
				assertTrue(rs.next());
				assertEquals("ї", rs.getString(1));
				assertEquals("\n", rs.getString(2));
				assertEquals("\\", rs.getString(3));
				assertEquals("don't", rs.getString(4));
			}
		}
	}

	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	@Test
	void shouldFetchBoolean() throws SQLException {
		try (Connection connection = createConnection()) {
			try (PreparedStatement statement = connection
					.prepareStatement("SELECT ? as a, ? as b, ? as c")) {
				statement.setBoolean(1, true);
				statement.setObject(2, false);
				statement.setObject(3, true, Types.BOOLEAN);
				statement.execute();
				ResultSet rs = statement.getResultSet();
				assertEquals(FireboltDataType.BOOLEAN.name().toLowerCase(),
						rs.getMetaData().getColumnTypeName(1).toLowerCase());
				assertEquals(FireboltDataType.BOOLEAN.name().toLowerCase(),
						rs.getMetaData().getColumnTypeName(2).toLowerCase());
				assertEquals(FireboltDataType.BOOLEAN.name().toLowerCase(),
						rs.getMetaData().getColumnTypeName(3).toLowerCase());
				assertTrue(rs.next());
				assertTrue(rs.getBoolean(1));
				assertFalse(rs.getBoolean(2));
				assertTrue(rs.getBoolean(3));
			}
		}
	}

	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	@Disabled
	@ParameterizedTest
	@MethodSource("com.firebolt.jdbc.testutils.TestFixtures#booleanTypes")
	void shouldFetchBooleanFromVariousObjects(Object objectTrue, Object objectFalse) throws SQLException {
		try (Connection connection = createConnection()) {
			try (PreparedStatement statement = connection
					.prepareStatement("SELECT ? as a, ? as b")) {
				statement.setObject(1, objectTrue, Types.BOOLEAN);
				statement.setObject(2, objectFalse, Types.BOOLEAN);
				statement.execute();
				ResultSet rs = statement.getResultSet();
				assertEquals(FireboltDataType.BOOLEAN.name().toLowerCase(),
						rs.getMetaData().getColumnTypeName(1).toLowerCase());
				assertEquals(FireboltDataType.BOOLEAN.name().toLowerCase(),
						rs.getMetaData().getColumnTypeName(2).toLowerCase());
				assertTrue(rs.next());
				assertTrue(rs.getBoolean(1));
				assertFalse(rs.getBoolean(2));
			}
		}
	}

	@Tag(TestTag.CORE)
	@Tag(TestTag.V2)
	@Test
	public void shouldFailAllPreparedStatementsFromBatchIfOneFails() throws SQLException {
		Map<String, String> connectionParams = Map.of("merge_prepared_statement_batches", "true");
		try (Connection connection = createConnection(ConnectionInfo.getInstance().getEngine(), connectionParams)) {
			try (PreparedStatement statement = connection
					.prepareStatement("INSERT INTO prepared_statement_test (sales, make) VALUES (?,?)")) {
				statement.setObject(1, "123");
				statement.setObject(2, "successful car1");
				statement.addBatch();
				statement.setObject(1, "not a big int");
				statement.setObject(2, "unsuccessful car");
				statement.addBatch();
				statement.setObject(1, "456");
				statement.setObject(2, "successful car2");
				FireboltException exception = assertThrows(FireboltException.class, () -> statement.executeBatch());
				assertEquals(exception.getErrorMessageFromServer(), "Unable to cast text 'not a big int' to bigint");
			}

			// verify that the successful rows were not inserted
			try (Statement statement = connection.createStatement()) {
				ResultSet resultSet = statement.executeQuery("SELECT * from prepared_statement_test where sales=123 and make='successful car1'");
				assertFalse(resultSet.next());
				resultSet = statement.executeQuery("SELECT * from prepared_statement_test where sales=456 and make='successful car2'");
				assertFalse(resultSet.next());
			}
		}
	}

	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	@Test
	void shouldInsertRecordsInBatchWithMergeV2() throws SQLException {
		Car car1 = Car.builder().make("BMW").sales(200).build();
		Car car2 = Car.builder().make("Ford").sales(150).build();
		Car car3 = Car.builder().make("Tesla").sales(300).build();
		Map<String, String> connectionParams = Map.of("merge_prepared_statement_batches_v2", "true");
		String currentUTCTime = getCurrentUTCTime();
		try (Connection connection = createConnection(ConnectionInfo.getInstance().getEngine(), connectionParams)) {

			try (PreparedStatement statement = connection
					.prepareStatement("INSERT INTO prepared_statement_test (sales, make) VALUES (?,?)")) {
				statement.setObject(1, car1.getSales());
				statement.setObject(2, car1.getMake());
				statement.addBatch();
				statement.setObject(1, car2.getSales());
				statement.setObject(2, car2.getMake());
				statement.addBatch();
				statement.setObject(1, car3.getSales());
				statement.setObject(2, car3.getMake());
				statement.addBatch();
				int[] result = statement.executeBatch();
				assertArrayEquals(new int[] { SUCCESS_NO_INFO, SUCCESS_NO_INFO, SUCCESS_NO_INFO }, result);
			}

			// Verify only 1 query was sent with merged VALUES
			verifySingleMergedQuery(connection, currentUTCTime, "VALUES (200,'BMW'), (150,'Ford'), (300,'Tesla')");

			List<List<?>> expectedRows = new ArrayList<>();
			expectedRows.add(Arrays.asList(car1.getSales(), car1.getMake()));
			expectedRows.add(Arrays.asList(car2.getSales(), car2.getMake()));
			expectedRows.add(Arrays.asList(car3.getSales(), car3.getMake()));

			QueryResult queryResult = createExpectedResult(expectedRows);

			try (Statement statement = connection.createStatement();
				 ResultSet rs = statement
						 .executeQuery("SELECT sales, make FROM prepared_statement_test ORDER BY make");
				 ResultSet expectedRs = FireboltResultSet.of(queryResult)) {
				AssertionUtil.assertResultSetEquality(expectedRs, rs);
			}
		}
	}

	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	@Test
	void shouldThrowExceptionWhenNotInsertWithMergeV2() throws SQLException {
		Map<String, String> connectionParams = Map.of("merge_prepared_statement_batches_v2", "true");
		try (Connection connection = createConnection(ConnectionInfo.getInstance().getEngine(), connectionParams)) {
			try (PreparedStatement statement = connection
					.prepareStatement("SELECT * FROM prepared_statement_test WHERE make = ?")) {
				statement.setObject(1, "Ford");
				statement.addBatch();
				statement.setObject(1, "Tesla");
				statement.addBatch();
				assertThrows(SQLException.class, () -> statement.executeBatch());
			}
		}
	}

	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	@Test
	void shouldInsertRecordsInBatchWithMergeV2AndTrailingWhitespaces() throws SQLException {
		Car car1 = Car.builder().make("Ford").sales(150).build();
		Car car2 = Car.builder().make("Tesla").sales(300).build();
		Map<String, String> connectionParams = Map.of("merge_prepared_statement_batches_v2", "true");
		String currentUTCTime = getCurrentUTCTime();
		try (Connection connection = createConnection(ConnectionInfo.getInstance().getEngine(), connectionParams)) {

			try (PreparedStatement statement = connection
					.prepareStatement("INSERT INTO prepared_statement_test (sales, make) VALUES (?,?)   ")) {
				statement.setObject(1, car1.getSales());
				statement.setObject(2, car1.getMake());
				statement.addBatch();
				statement.setObject(1, car2.getSales());
				statement.setObject(2, car2.getMake());
				statement.addBatch();
				int[] result = statement.executeBatch();
				assertArrayEquals(new int[] { SUCCESS_NO_INFO, SUCCESS_NO_INFO }, result);
			}

			// Verify only 1 query was sent with merged VALUES
			verifySingleMergedQuery(connection, currentUTCTime, "VALUES (150,'Ford'), (300,'Tesla')");

			List<List<?>> expectedRows = new ArrayList<>();
			expectedRows.add(Arrays.asList(car1.getSales(), car1.getMake()));
			expectedRows.add(Arrays.asList(car2.getSales(), car2.getMake()));

			QueryResult queryResult = createExpectedResult(expectedRows);

			try (Statement statement = connection.createStatement();
				 ResultSet rs = statement
						 .executeQuery("SELECT sales, make FROM prepared_statement_test WHERE make IN ('Ford', 'Tesla') ORDER BY make");
				 ResultSet expectedRs = FireboltResultSet.of(queryResult)) {
				AssertionUtil.assertResultSetEquality(expectedRs, rs);
			}
		}
	}

	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	@Test
	void shouldInsertRecordsInBatchWithMergeV2AndNewlines() throws SQLException {
		Car car1 = Car.builder().make("Audi").sales(250).build();
		Car car2 = Car.builder().make("BMW").sales(200).build();
		Map<String, String> connectionParams = Map.of("merge_prepared_statement_batches_v2", "true");
		String currentUTCTime = getCurrentUTCTime();
		try (Connection connection = createConnection(ConnectionInfo.getInstance().getEngine(), connectionParams)) {

			try (PreparedStatement statement = connection
					.prepareStatement("INSERT INTO prepared_statement_test (sales, make)\nVALUES (?,?)\n")) {
				statement.setObject(1, car1.getSales());
				statement.setObject(2, car1.getMake());
				statement.addBatch();
				statement.setObject(1, car2.getSales());
				statement.setObject(2, car2.getMake());
				statement.addBatch();
				int[] result = statement.executeBatch();
				assertArrayEquals(new int[] { SUCCESS_NO_INFO, SUCCESS_NO_INFO }, result);
			}

			// Verify only 1 query was sent with merged VALUES
			verifySingleMergedQuery(connection, currentUTCTime, "VALUES (250,'Audi'), (200,'BMW')");

			List<List<?>> expectedRows = new ArrayList<>();
			expectedRows.add(Arrays.asList(car1.getSales(), car1.getMake()));
			expectedRows.add(Arrays.asList(car2.getSales(), car2.getMake()));

			QueryResult queryResult = createExpectedResult(expectedRows);

			try (Statement statement = connection.createStatement();
				 ResultSet rs = statement
						 .executeQuery("SELECT sales, make FROM prepared_statement_test WHERE make IN ('BMW', 'Audi') ORDER BY make");
				 ResultSet expectedRs = FireboltResultSet.of(queryResult)) {
				AssertionUtil.assertResultSetEquality(expectedRs, rs);
			}
		}
	}

	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	@Test
	void shouldInsertRecordsInBatchWithMergeV2AndMultipleWhitespaces() throws SQLException {
		Car car1 = Car.builder().make("Mercedes").sales(350).build();
		Car car2 = Car.builder().make("Porsche").sales(400).build();
		Map<String, String> connectionParams = Map.of("merge_prepared_statement_batches_v2", "true");
		String currentUTCTime = getCurrentUTCTime();
		try (Connection connection = createConnection(ConnectionInfo.getInstance().getEngine(), connectionParams)) {

			try (PreparedStatement statement = connection
					.prepareStatement("INSERT INTO prepared_statement_test (sales, make)    VALUES    (?,?)")) {
				statement.setObject(1, car1.getSales());
				statement.setObject(2, car1.getMake());
				statement.addBatch();
				statement.setObject(1, car2.getSales());
				statement.setObject(2, car2.getMake());
				statement.addBatch();
				int[] result = statement.executeBatch();
				assertArrayEquals(new int[] { SUCCESS_NO_INFO, SUCCESS_NO_INFO }, result);
			}

			// Verify only 1 query was sent with merged VALUES
			verifySingleMergedQuery(connection, currentUTCTime, "VALUES    (350,'Mercedes'), (400,'Porsche')");

			List<List<?>> expectedRows = new ArrayList<>();
			expectedRows.add(Arrays.asList(car1.getSales(), car1.getMake()));
			expectedRows.add(Arrays.asList(car2.getSales(), car2.getMake()));

			QueryResult queryResult = createExpectedResult(expectedRows);

			try (Statement statement = connection.createStatement();
				 ResultSet rs = statement
						 .executeQuery("SELECT sales, make FROM prepared_statement_test WHERE make IN ('Mercedes', 'Porsche') ORDER BY make");
				 ResultSet expectedRs = FireboltResultSet.of(queryResult)) {
				AssertionUtil.assertResultSetEquality(expectedRs, rs);
			}
		}
	}

	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	@Test
	void shouldInsertRecordsInBatchWithMergeV2TrailingWhitespacesAndSemicolon() throws SQLException {
		Car car1 = Car.builder().make("Jaguar").sales(180).build();
		Car car2 = Car.builder().make("Volvo").sales(320).build();
		Map<String, String> connectionParams = Map.of("merge_prepared_statement_batches_v2", "true");
		String currentUTCTime = getCurrentUTCTime();
		try (Connection connection = createConnection(ConnectionInfo.getInstance().getEngine(), connectionParams)) {

			try (PreparedStatement statement = connection
					.prepareStatement("INSERT INTO prepared_statement_test (sales, make) VALUES (?,?)   ;")) {
				statement.setObject(1, car1.getSales());
				statement.setObject(2, car1.getMake());
				statement.addBatch();
				statement.setObject(1, car2.getSales());
				statement.setObject(2, car2.getMake());
				statement.addBatch();
				int[] result = statement.executeBatch();
				assertArrayEquals(new int[] { SUCCESS_NO_INFO, SUCCESS_NO_INFO }, result);
			}

			// Verify only 1 query was sent with merged VALUES
			verifySingleMergedQuery(connection, currentUTCTime, "VALUES (180,'Jaguar'), (320,'Volvo')");

			List<List<?>> expectedRows = new ArrayList<>();
			expectedRows.add(Arrays.asList(car1.getSales(), car1.getMake()));
			expectedRows.add(Arrays.asList(car2.getSales(), car2.getMake()));

			QueryResult queryResult = createExpectedResult(expectedRows);

			try (Statement statement = connection.createStatement();
				 ResultSet rs = statement
						 .executeQuery("SELECT sales, make FROM prepared_statement_test WHERE make IN ('Volvo', 'Jaguar') ORDER BY make");
				 ResultSet expectedRs = FireboltResultSet.of(queryResult)) {
				AssertionUtil.assertResultSetEquality(expectedRs, rs);
			}
		}
	}

	@Tag(TestTag.V1)
	@Tag(TestTag.V2)
	@Tag(TestTag.CORE)
	@Test
	void shouldInsertRecordsInBatchWithMergeV2NewlinesWhitespacesAndSemicolon() throws SQLException {
		Car car1 = Car.builder().make("Infiniti").sales(280).build();
		Car car2 = Car.builder().make("Lexus").sales(270).build();
		Map<String, String> connectionParams = Map.of("merge_prepared_statement_batches_v2", "true");
		String currentUTCTime = getCurrentUTCTime();
		try (Connection connection = createConnection(ConnectionInfo.getInstance().getEngine(), connectionParams)) {

			try (PreparedStatement statement = connection
					.prepareStatement("INSERT INTO prepared_statement_test (sales, make)\nVALUES (?,?)  ;     ")) {
				statement.setObject(1, car1.getSales());
				statement.setObject(2, car1.getMake());
				statement.addBatch();
				statement.setObject(1, car2.getSales());
				statement.setObject(2, car2.getMake());
				statement.addBatch();
				int[] result = statement.executeBatch();
				assertArrayEquals(new int[] { SUCCESS_NO_INFO, SUCCESS_NO_INFO }, result);
			}

			// Verify only 1 query was sent with merged VALUES
			verifySingleMergedQuery(connection, currentUTCTime, "VALUES (280,'Infiniti'), (270,'Lexus')");

			List<List<?>> expectedRows = new ArrayList<>();
			expectedRows.add(Arrays.asList(car1.getSales(), car1.getMake()));
			expectedRows.add(Arrays.asList(car2.getSales(), car2.getMake()));

			QueryResult queryResult = createExpectedResult(expectedRows);

			try (Statement statement = connection.createStatement();
				 ResultSet rs = statement
						 .executeQuery("SELECT sales, make FROM prepared_statement_test WHERE make IN ('Infiniti', 'Lexus') ORDER BY make");
				 ResultSet expectedRs = FireboltResultSet.of(queryResult)) {
				AssertionUtil.assertResultSetEquality(expectedRs, rs);
			}
		}
	}

	/**
	 * Verifies that only one INSERT query was sent to the server and it contains the expected merged VALUES.
	 *
	 * @param connection The database connection
	 * @param currentUTCTime The UTC time before the batch was executed
	 * @param expectedValuesContent The expected VALUES content (e.g., "VALUES (150,'Ford'), (300,'Tesla')")
	 * @throws SQLException if the verification fails
	 */
	private void verifySingleMergedQuery(Connection connection, String currentUTCTime, String expectedValuesContent) throws SQLException {
		sleepForMillis(TimeUnit.SECONDS.toMillis(8)); // Wait for query history to propagate
		try (Statement statement = connection.createStatement()) {
			String queryHistoryQuery = String.format(
					"SELECT query_text FROM information_schema.engine_query_history " +
					"WHERE submitted_time > '%s' AND query_text LIKE 'INSERT INTO prepared_statement_test%%' " +
					"AND status = 'STARTED_EXECUTION' ORDER BY submitted_time DESC",
					currentUTCTime);
			try (ResultSet rs = statement.executeQuery(queryHistoryQuery)) {
				assertTrue(rs.next(), "Should have at least one INSERT query");
				String queryText = rs.getString(1);
				// Verify it's the merged query with expected VALUES
				assertTrue(queryText.contains(expectedValuesContent),
						"Query should contain merged VALUES: " + queryText);
				// Verify only one INSERT query was executed
				assertFalse(rs.next(), "Should have only one INSERT query, but found more");
			}
		}
	}

	Stream<Arguments> dateTypes() {
		return Stream.of(
				Arguments.of(LocalDateTime.of(2019, 7, 31, 11, 15, 13),
						LocalDate.of(2019, 7, 31), true),
				Arguments.of(new Timestamp(1564571713000L),
						new Date(1564571713000L), true),
				Arguments.of(LocalDateTime.of(2019, 7, 31, 11, 15, 13),
						LocalDate.of(2019, 7, 31), false),
				Arguments.of(new Timestamp(1564571713000L),
						new Date(1564571713000L), false)
		);
	}

	@Builder
	@Value
	@EqualsAndHashCode
	private static class Car {
		Integer sales;
		String make;
		byte[] signature;
		Timestamp ts;
		Date d;
		URL url;
		String[] tags;
	}

}
