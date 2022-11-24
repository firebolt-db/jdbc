package integration.tests;

import integration.IntegrationTest;
import lombok.CustomLog;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.DefaultTimeZone;

import java.sql.*;
import java.time.ZonedDateTime;
import java.util.TimeZone;

import static com.firebolt.jdbc.type.date.SqlDateUtil.ONE_DAY_MILLIS;
import static org.junit.jupiter.api.Assertions.assertEquals;

@CustomLog
@DefaultTimeZone("UTC")
class TimestampTest extends IntegrationTest {

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

}
