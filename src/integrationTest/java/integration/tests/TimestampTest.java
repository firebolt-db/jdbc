package integration.tests;

import static com.firebolt.jdbc.type.date.SqlDateUtil.ONE_DAY_MILLIS;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.sql.*;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.TimeZone;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.DefaultTimeZone;

import com.firebolt.jdbc.testutils.AssertionUtil;

import integration.IntegrationTest;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import lombok.CustomLog;

@CustomLog
@DefaultTimeZone("UTC")
class TimestampTest extends IntegrationTest {
	private static final TimeZone UTC_TZ = TimeZone.getTimeZone("UTC");
	private static final Calendar EST_CALENDAR = Calendar.getInstance(TimeZone.getTimeZone("EST"));

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
	void shouldGetTimeObjectsInEstTimezone() throws SQLException {
		try (Connection connection = this.createConnection();
				Statement statement = connection.createStatement();
				ResultSet resultSet = statement
						.executeQuery("SELECT TO_TIMESTAMP_EXT('1975/01/01 23:01:01', 6, 'EST');")) {
			resultSet.next();

			ZonedDateTime zonedDateTime = ZonedDateTime.of(1975, 1, 2, 4, 1, 1, 0,
					TimeZone.getTimeZone("UTC").toZoneId());

			Timestamp expectedTimestamp = new Timestamp(zonedDateTime.toInstant().toEpochMilli());
			Time expectedTime = new Time(zonedDateTime.toInstant().toEpochMilli());
			Date expectedDate = new Date(zonedDateTime.toInstant().toEpochMilli());

			assertEquals(expectedTimestamp, resultSet.getTimestamp(1));
			assertEquals(expectedTimestamp, resultSet.getObject(1));
			assertEquals(expectedTime, resultSet.getTime(1));
			assertEquals(expectedDate, resultSet.getDate(1));
		}
	}

	@Test
	void shouldGetTimeObjectsInDefaultUTCTimezone() throws SQLException {
		try (Connection connection = this.createConnection();
				Statement statement = connection.createStatement();
				ResultSet resultSet = statement.executeQuery("SELECT TO_TIMESTAMP_EXT('1975/01/01 23:01:01', 6);")) {
			resultSet.next();
			ZonedDateTime zonedDateTime = ZonedDateTime.of(1975, 1, 1, 23, 1, 1, 0,
					TimeZone.getTimeZone("UTC").toZoneId());

			Timestamp expectedTimestamp = new Timestamp(zonedDateTime.toInstant().toEpochMilli());
			Time expectedTime = new Time(zonedDateTime.toInstant().toEpochMilli());
			Date expectedDate = new Date(zonedDateTime.toInstant().toEpochMilli());

			assertEquals(expectedTimestamp, resultSet.getTimestamp(1));
			assertEquals(expectedTimestamp, resultSet.getObject(1));
			assertEquals(expectedTime, resultSet.getTime(1));
			assertEquals(expectedDate, resultSet.getDate(1));
		}
	}

	@Test
	void shouldGetParsedTimeStampExtTimeObjects() throws SQLException {
		try (Connection connection = this.createConnection();
				Statement statement = connection.createStatement();
				ResultSet resultSet = statement
						.executeQuery("SELECT CAST('1111-11-11 ' || '12:00:03' AS timestamp_ext);")) {
			resultSet.next();
			ZonedDateTime zonedDateTime = ZonedDateTime.of(1111, 11, 11, 12, 0, 3, 0,
					TimeZone.getTimeZone("UTC").toZoneId());

			Timestamp expectedTimestamp = new Timestamp(zonedDateTime.toInstant().toEpochMilli() + 7 * ONE_DAY_MILLIS);
			Time expectedTime = new Time(zonedDateTime.toInstant().toEpochMilli() + 7 * ONE_DAY_MILLIS);
			Date expectedDate = new Date(zonedDateTime.toInstant().toEpochMilli() + 7 * ONE_DAY_MILLIS);

			assertEquals(expectedTimestamp, resultSet.getTimestamp(1));
			assertEquals(expectedTimestamp, resultSet.getObject(1));
			assertEquals(expectedTime, resultSet.getTime(1));
			assertEquals(expectedDate, resultSet.getDate(1));
			assertEquals(expectedTimestamp, resultSet.getTimestamp(1));
		}
	}

	@Test
	void shouldReturnTimestampFromTimestampntz() throws SQLException {
		try (Connection connection = this.createConnection();
				Statement statement = connection.createStatement();
				ResultSet resultSet = statement.executeQuery("SELECT '2022-05-10 23:01:02.123'::timestampntz;")) {
			resultSet.next();
			ZonedDateTime expectedZdt = ZonedDateTime.of(2022, 5, 10, 23, 1, 2, 123, UTC_TZ.toZoneId());
			Date expectedDate = new Date(expectedZdt.truncatedTo(ChronoUnit.DAYS).toInstant().toEpochMilli());
			Time expectedTime = new Time(82862123);

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
		try (Connection connection = this.createConnection();
		Statement statement = connection.createStatement();
		Statement statementWithTzInQuery = connection.createStatement();
		ResultSet resultSetWithTzInQuery = statementWithTzInQuery.executeQuery("SELECT '2022-05-10 23:01:02.123 Europe/Berlin'::timestamptz;"))
		{
			//Same as: SELECT '2022-05-10 23:01:02.123 Europe/Berlin'::timestamptz;
			statement.execute("SET advanced_mode=1;SET time_zone = 'Europe/Berlin';");
			ResultSet resultSetWithTzAsQueryParam = statement.executeQuery("SELECT '2022-05-10 23:01:02.123'::timestamptz;");
			resultSetWithTzInQuery.next();
			resultSetWithTzAsQueryParam.next();
			ZonedDateTime expectedZdt = ZonedDateTime.of(2022, 5, 10, 21, 1, 2, 123, UTC_TZ.toZoneId());
			Date expectedDate = new Date(expectedZdt.truncatedTo(ChronoUnit.DAYS).toInstant().toEpochMilli());
			Time expectedTime = new Time(75662123); // milliseconds at 1970-01-01T21:01:02.123Z since January 1, 1970,
													// 00:00:00 GMT

			Timestamp expectedTimestamp = new Timestamp(expectedZdt.toInstant().toEpochMilli());
			expectedTimestamp.setNanos(123000000);

			assertEquals(expectedTimestamp, resultSetWithTzInQuery.getTimestamp(1));
			assertEquals(expectedTimestamp, resultSetWithTzInQuery.getTimestamp(1, EST_CALENDAR));
			assertEquals(expectedTimestamp, resultSetWithTzInQuery.getObject(1));
			assertEquals(expectedTime, resultSetWithTzInQuery.getTime(1));
			assertEquals(expectedDate, resultSetWithTzInQuery.getDate(1));
			assertEquals(expectedTimestamp, resultSetWithTzInQuery.getTimestamp(1));
			compareAllDateTimeResultSetValuesWithPostgres(resultSetWithTzInQuery, "SELECT '2022-05-10 23:01:02.123 Europe/Berlin'::timestamptz;");

			//verifies that setting the timezone as a query param or directly within the SELECT statement gives the same result
			AssertionUtil.assertResultSetValuesEquality(resultSetWithTzAsQueryParam, resultSetWithTzInQuery);
			resultSetWithTzAsQueryParam.close();
		}
	}

	@Test
	void shouldReturnTimestampFromDate() throws SQLException {
		try (Connection connection = this.createConnection();
				Statement statement = connection.createStatement()) {
			statement.execute("SET advanced_mode=1; SET time_zone='Europe/Berlin';");
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

			compareAllDateTimeResultSetValuesWithPostgres(resultSet, "SELECT '2022-05-10'::date;");

		}
	}

	private void compareAllDateTimeResultSetValuesWithPostgres(ResultSet fireboltResultSet, String postgresQuery)
			throws SQLException {
		try (Connection postgresConnection = embeddedPostgres.getPostgresDatabase().getConnection();
				Statement pgStatement = postgresConnection.createStatement();
				ResultSet postgresResultSet = pgStatement.executeQuery(postgresQuery)) {
			postgresResultSet.next();
			assertEquals(postgresResultSet.getObject(1), fireboltResultSet.getObject(1));
			assertEquals(postgresResultSet.getTime(1), fireboltResultSet.getTime(1));
			assertEquals(postgresResultSet.getTime(1, EST_CALENDAR), fireboltResultSet.getTime(1, EST_CALENDAR));
			assertEquals(postgresResultSet.getDate(1), fireboltResultSet.getDate(1));
			assertEquals(postgresResultSet.getDate(1, EST_CALENDAR), fireboltResultSet.getDate(1, EST_CALENDAR));
			assertEquals(postgresResultSet.getTimestamp(1), fireboltResultSet.getTimestamp(1));
			assertEquals(postgresResultSet.getTimestamp(1, EST_CALENDAR),
					fireboltResultSet.getTimestamp(1, EST_CALENDAR));
		}
	}

}
