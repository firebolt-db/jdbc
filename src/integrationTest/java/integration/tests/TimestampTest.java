package integration.tests;

import com.firebolt.jdbc.testutils.AssertionUtil;
import integration.IntegrationTest;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.CustomLog;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.DefaultTimeZone;

import static com.firebolt.jdbc.type.date.SqlDateUtil.ONE_DAY_MILLIS;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TIMESTAMP_WITH_TIMEZONE;
import static org.junit.jupiter.api.Assertions.assertEquals;

@CustomLog
@DefaultTimeZone("UTC")
class TimestampTest extends IntegrationTest {
	private static final TimeZone UTC_TZ = TimeZone.getTimeZone("UTC");
	private static final Calendar EST_CALENDAR = Calendar.getInstance(TimeZone.getTimeZone("EST"));
	private static final Calendar ECT_CALENDAR = Calendar.getInstance(TimeZone.getTimeZone("ECT"));

	private EmbeddedPostgres embeddedPostgres;

	@BeforeAll
	void init() throws IOException {
		embeddedPostgres = EmbeddedPostgres.start();
	}

	@AfterAll
	void tearDown() throws IOException {
		embeddedPostgres.close();
	}

	@Test
	void shouldGetTimeObjectsInDefaultUTCTimezone() throws SQLException {
		try (Connection connection = createConnection();
			 Statement statement = connection.createStatement();
			 ResultSet resultSet = statement.executeQuery("SELECT TO_TIMESTAMP('1975/01/01 23:01:01', 'yyyy/MM/DD hh24:mi:ss');")) {
			resultSet.next();
			ZonedDateTime zonedDateTime = ZonedDateTime.of(1975, 1, 1, 23, 1, 1, 0,
					TimeZone.getTimeZone("UTC").toZoneId());
			ZonedDateTime expectedTimeZoneDateTime = ZonedDateTime.of(1970, 1, 1, 23, 1, 1, 0,
					TimeZone.getTimeZone("UTC").toZoneId());

			Timestamp expectedTimestamp = new Timestamp(zonedDateTime.toInstant().toEpochMilli());
			Time expectedTime = new Time(expectedTimeZoneDateTime.toInstant().toEpochMilli());
			Date expectedDate = new Date(zonedDateTime.truncatedTo(ChronoUnit.DAYS).toInstant().toEpochMilli());

			assertEquals(expectedTimestamp, resultSet.getTimestamp(1));
			assertEquals(expectedTimestamp, resultSet.getObject(1));
			assertEquals(expectedTime, resultSet.getTime(1));
			assertEquals(expectedDate, resultSet.getDate(1));
		}
	}

	@Test
	void shouldGetParsedTimeStampExtTimeObjects() throws SQLException {
		try (Connection connection = createConnection();
			 Statement statement = connection.createStatement();
			 ResultSet resultSet = statement.executeQuery("SELECT CAST('1111-11-11 ' || '12:00:03' AS timestamptz);")) {
			resultSet.next();
			ZonedDateTime expectedTimestampZdt = ZonedDateTime.of(1111, 11, 11, 12, 0, 3, 0,
					TimeZone.getTimeZone("UTC").toZoneId());

			ZonedDateTime expectedTimeZdt = ZonedDateTime.of(1970, 1, 1, 12, 0, 3, 0,
					TimeZone.getTimeZone("UTC").toZoneId());
			Time expectedTime = new Time(expectedTimeZdt.toInstant().toEpochMilli());
			Date expectedDate = new Date(
					expectedTimestampZdt.truncatedTo(ChronoUnit.DAYS).toInstant().toEpochMilli() + 7 * ONE_DAY_MILLIS);

			Timestamp expectedTimestamp = new Timestamp(
					expectedTimestampZdt.toInstant().toEpochMilli() + 7 * ONE_DAY_MILLIS);
			assertEquals(expectedTimestamp, resultSet.getTimestamp(1));
			assertEquals(expectedTimestamp, resultSet.getObject(1));
			assertEquals(expectedTime, resultSet.getTime(1));
			assertEquals(expectedDate, resultSet.getDate(1));
			assertEquals(expectedTimestamp, resultSet.getTimestamp(1));
		}
	}

	@Test
	@DefaultTimeZone("Asia/Kolkata")
	void shouldRemoveOffsetDIffWhenTimestampOffsetHasChanged() throws SQLException {
		// Asia/Kolkata had an offset of +05:21:10 in 1899 vs +05:30 today. The
		// timestamp returned should have the time 00:00:00 (so without the difference
		// of 08:50).
		try (Connection connection = createConnection();
				Statement statement = connection.createStatement();
				ResultSet resultSet = statement.executeQuery("SELECT CAST('1899-01-01 00:00:00' AS timestampntz);")) {
			resultSet.next();
			long offsetDiffInMillis = ((8 * 60) + 50) * 1000L; // 8:50 in millis
			ZonedDateTime expectedTimestampZdt = ZonedDateTime.of(1899, 1, 1, 0, 0, 0, 0,
					TimeZone.getTimeZone("Asia/Kolkata").toZoneId());
			Timestamp expectedTimestamp = new Timestamp(
					expectedTimestampZdt.toInstant().toEpochMilli() - offsetDiffInMillis);
			assertEquals(expectedTimestamp, resultSet.getTimestamp(1));
			// Timestamp returned from PostgreSQL has the difference of 8:50
			// TODO discover why this happen and uncomment one of these lines
			//compareAllDateTimeResultSetValuesWithPostgres(resultSet, "SELECT '1899-01-01 00:00:00'::timestamp with time zone", "Asia/Kolkata");
			//compareAllDateTimeResultSetValuesWithPostgres(resultSet, "SELECT '1899-01-01 00:00:00'::timestamp with time zone");
		}
	}

	@Test
	@DefaultTimeZone("CET")
	void shouldRemoveOffsetDIffWhenTimestampOffsetHasChangedCET() throws SQLException {
		// Asia/Kolkata had an offset of +05:21:10 in 1899 vs +05:30 today. The
		// timestamp returned should have the time 00:00:00 (so without the difference
		// of 08:50).
		try (Connection connection = createConnection();
				Statement statement = connection.createStatement();
				ResultSet resultSet = statement.executeQuery("SELECT CAST('2100-05-11 00:00:00' AS timestamp);")) {
			resultSet.next();
			compareAllDateTimeResultSetValuesWithPostgres(resultSet, "SELECT '2100-05-11 00:00:00'::timestamp");
		}
	}

	@Test
	void shouldReturnTimestampFromTimestampntz() throws SQLException {
		try (Connection connection = createConnection();
				Statement statement = connection.createStatement();
				ResultSet resultSet = statement.executeQuery("SELECT '2022-05-10 23:01:02.123'::timestampntz;")) {
			resultSet.next();
			ZonedDateTime expectedZdt = ZonedDateTime.of(2022, 5, 10, 23, 1, 2, 0, UTC_TZ.toZoneId());
			Date expectedDate = new Date(expectedZdt.truncatedTo(ChronoUnit.DAYS).toInstant().toEpochMilli());
			Time expectedTime = new Time(82862123); // 1970-01-01T23:01:02.123Z

			Timestamp expectedTimestamp = new Timestamp(expectedZdt.toInstant().toEpochMilli());
			expectedTimestamp.setNanos(123000000);

			assertEquals(expectedTimestamp, resultSet.getTimestamp(1));
			assertEquals(expectedTimestamp, resultSet.getObject(1));
			assertEquals(expectedTime, resultSet.getTime(1));
			assertEquals(expectedDate, resultSet.getDate(1));
			assertEquals(expectedTimestamp, resultSet.getTimestamp(1));
			compareAllDateTimeResultSetValuesWithPostgres(resultSet, "SELECT '2022-05-10 23:01:02.123'::timestamp;");
		}
	}

	@Test
	void shouldReturnTimestampFromTimestamptz() throws SQLException {
		try (Connection connection = createConnection();
				Statement statement = connection.createStatement();
				Statement statementWithTzInQuery = connection.createStatement();
				ResultSet resultSetWithTzInQuery = statementWithTzInQuery
						.executeQuery("SELECT '1975-05-10 23:01:02.123 EST'::timestamptz;")) {
			// Same as: SELECT '1975-05-10 23:01:02.123 Europe/Berlin'::timestamptz;
			statement.execute("SET time_zone = 'EST';");
			ResultSet resultSetWithTzAsQueryParam = statement
					.executeQuery("SELECT '1975-05-10 23:01:02.123'::timestamptz;");
			resultSetWithTzInQuery.next();
			resultSetWithTzAsQueryParam.next();
			ZonedDateTime expectedZdt = ZonedDateTime.of(1975, 5, 11, 4, 1, 2, 0, UTC_TZ.toZoneId());
			Date expectedDate = new Date(expectedZdt.truncatedTo(ChronoUnit.DAYS).toInstant().toEpochMilli());
			Time expectedTime = new Time(14462123); // milliseconds at 1970-01-01T04:01:02.123Z since January 1, 1970,
													// 00:00:00 GMT

			Timestamp expectedTimestamp = new Timestamp(expectedZdt.toInstant().toEpochMilli());
			expectedTimestamp.setNanos(123000000);

			assertEquals(expectedTimestamp, resultSetWithTzInQuery.getTimestamp(1));
			assertEquals(expectedTimestamp, resultSetWithTzInQuery.getTimestamp(1, EST_CALENDAR));
			assertEquals(expectedTimestamp, resultSetWithTzInQuery.getObject(1));
			assertEquals(expectedTime, resultSetWithTzInQuery.getTime(1));
			assertEquals(expectedDate, resultSetWithTzInQuery.getDate(1));
			assertEquals(expectedTimestamp, resultSetWithTzInQuery.getTimestamp(1));
			compareAllDateTimeResultSetValuesWithPostgres(resultSetWithTzInQuery,
					"SELECT '1975-05-10 23:01:02.123 EST'::timestamptz;");

			// verifies that setting the timezone as a query param or directly within the
			// SELECT statement gives the same result
			AssertionUtil.assertResultSetValuesEquality(resultSetWithTzAsQueryParam, resultSetWithTzInQuery);
			resultSetWithTzAsQueryParam.close();
		}
	}

	@Test
	void shouldReturnTimestampFromTimestampTzWithTzWithHoursAndMinutes() throws SQLException {
		try (Connection connection = createConnection(); Statement statement = connection.createStatement()) {
			statement.execute("SET time_zone = 'Asia/Calcutta';"); // The server will return a tz in
			// the format +05:30
			ResultSet resultSet = statement.executeQuery("SELECT '1975-05-10 23:01:02.123'::timestamptz;");
			resultSet.next();
			compareAllDateTimeResultSetValuesWithPostgres(resultSet, "SELECT '1975-05-10 23:01:02.123'::timestamptz;",
					"Asia/Calcutta");
		}
	}

	@Test
	void shouldReturnTimestampFromTimestampTzWithTzWithHoursAndMinutesAndSeconds() throws SQLException {
		try (Connection connection = createConnection(); Statement statement = connection.createStatement()) {
			statement.execute("SET time_zone = 'Asia/Calcutta';"); // The server will return a tz in
			// the format +05:30
			ResultSet resultSet = statement.executeQuery("SELECT '1111-01-05 17:04:42.123456'::timestamptz");
			resultSet.next();
			Timestamp expectedTimestamp = new Timestamp(
					ZonedDateTime.of(1111, 1, 5, 11, 11, 14, 0, UTC_TZ.toZoneId()).toInstant().toEpochMilli()
							+ 7 * ONE_DAY_MILLIS);
			expectedTimestamp.setNanos(123456000);
			assertEquals(expectedTimestamp, resultSet.getObject(1));
			compareAllDateTimeResultSetValuesWithPostgres(resultSet, "SELECT '1111-01-05 17:04:42.123456'::timestamptz",
					"Asia/Calcutta");
		}
	}

	@Test
	void shouldReturnTimestampFromDate() throws SQLException {
		try (Connection connection = createConnection(); Statement statement = connection.createStatement()) {
			statement.execute("SET time_zone='Europe/Berlin';");
			ResultSet resultSet = statement.executeQuery("SELECT '2022-05-10'::pgdate;");
			resultSet.next();
			ZonedDateTime expectedZdt = ZonedDateTime.of(2022, 5, 10, 0, 0, 0, 0, UTC_TZ.toZoneId());
			Date expectedDate = new Date(expectedZdt.truncatedTo(ChronoUnit.DAYS).toInstant().toEpochMilli());
			Time expectedTime = new Time(0);

			Timestamp expectedTimestamp = new Timestamp(expectedZdt.toInstant().toEpochMilli());

			assertEquals(expectedDate, resultSet.getObject(1));
			assertEquals(expectedTime, resultSet.getTime(1));
			assertEquals(expectedDate, resultSet.getDate(1));
			assertEquals(expectedTimestamp, resultSet.getTimestamp(1));

			compareAllDateTimeResultSetValuesWithPostgres(resultSet, "SELECT '2022-05-10'::date;", "Europe/Berlin");

		}
	}

	@Test
	void shouldCompareAllTimeStampsWithMultipleThreads() throws SQLException, InterruptedException, ExecutionException {
		try (Connection connection = createConnection();
				Statement statement = connection.createStatement();
				ResultSet resultSet = statement.executeQuery("SELECT CAST('1899-01-01 00:00:00' AS timestamptz);")) {
			resultSet.next();
			AtomicInteger count = new AtomicInteger(0);
			int expectedCount = 1000;
			Callable<Void> callable = () -> {
				compareAllDateTimeResultSetValuesWithPostgres(resultSet, "SELECT '1899-01-01 00:00:00'::timestamptz");
				count.incrementAndGet();
				return null;
			};
			List<Callable<Void>> callables = new ArrayList<>();
			for (int i = 0; i < expectedCount; i++) {
				callables.add(callable);
			}
			ExecutorService threadPool = Executors.newFixedThreadPool(10);
			List<Future<Void>> futures = threadPool.invokeAll(callables);
			for (Future<Void> future : futures) {
				future.get();
			}
			assertEquals(expectedCount, count.get());
			threadPool.shutdown();
		}
	}

	private void compareAllDateTimeResultSetValuesWithPostgres(ResultSet fireboltResultSet, String postgresQuery)
			throws SQLException {
		compareAllDateTimeResultSetValuesWithPostgres(fireboltResultSet, postgresQuery, null);
	}

	private void compareAllDateTimeResultSetValuesWithPostgres(ResultSet fireboltResultSet, String postgresQuery,
			String timezone) throws SQLException {
		try (Connection postgresConnection = embeddedPostgres.getPostgresDatabase().getConnection();
				Statement pgStatement = postgresConnection.createStatement()) {
			if (timezone != null) {
				pgStatement.execute(String.format("set timezone = '%s'", timezone));
			}

			ResultSet postgresResultSet = pgStatement.executeQuery(postgresQuery);
			postgresResultSet.next();
			assertEquals(postgresResultSet.getObject(1), fireboltResultSet.getObject(1));
			assertEquals(postgresResultSet.getTime(1), fireboltResultSet.getTime(1));
			assertEquals(postgresResultSet.getTime(1, EST_CALENDAR), fireboltResultSet.getTime(1, EST_CALENDAR));
			assertEquals(postgresResultSet.getTime(1, ECT_CALENDAR), fireboltResultSet.getTime(1, ECT_CALENDAR));

			assertEquals(postgresResultSet.getDate(1), fireboltResultSet.getDate(1));
			assertEquals(postgresResultSet.getDate(1, EST_CALENDAR), fireboltResultSet.getDate(1, EST_CALENDAR));
			assertEquals(postgresResultSet.getDate(1, ECT_CALENDAR), fireboltResultSet.getDate(1, ECT_CALENDAR));

			assertEquals(postgresResultSet.getTimestamp(1), fireboltResultSet.getTimestamp(1));
			assertEquals(postgresResultSet.getTimestamp(1, EST_CALENDAR),
					fireboltResultSet.getTimestamp(1, EST_CALENDAR));

			assertEquals(postgresResultSet.getTimestamp(1, ECT_CALENDAR),
					fireboltResultSet.getTimestamp(1, ECT_CALENDAR));
			if (Arrays.asList(TIMESTAMP_WITH_TIMEZONE, TIMESTAMP)
					.contains(postgresResultSet.getMetaData().getColumnType(1))) {
				assertEquals(postgresResultSet.getObject(1, OffsetDateTime.class),
						fireboltResultSet.getObject(1, OffsetDateTime.class));
			}
		}
	}

}
