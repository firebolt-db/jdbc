package integration.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.*;
import java.time.ZonedDateTime;
import java.util.TimeZone;

import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.DefaultTimeZone;

import integration.IntegrationTest;
import lombok.extern.slf4j.Slf4j;

@Slf4j
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

			Timestamp expectedTimestamp = Timestamp.valueOf(zonedDateTime.toLocalDateTime());
			Time expectedTime = Time.valueOf(zonedDateTime.toLocalTime());
			Date expectedDate = Date.valueOf(zonedDateTime.toLocalDate());

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

			Timestamp expectedTimestamp = Timestamp.valueOf(zonedDateTime.toLocalDateTime());
			Time expectedTime = Time.valueOf(zonedDateTime.toLocalTime());
			Date expectedDate = Date.valueOf(zonedDateTime.toLocalDate());

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

			Timestamp expectedTimestamp = Timestamp.valueOf(zonedDateTime.toLocalDateTime());
			Time expectedTime = Time.valueOf(zonedDateTime.toLocalTime());
			Date expectedDate = Date.valueOf(zonedDateTime.toLocalDate());

			assertEquals(expectedTimestamp, resultSet.getTimestamp(1));
			assertEquals(expectedTimestamp, resultSet.getObject(1));
			assertEquals(expectedTime, resultSet.getTime(1));
			assertEquals(expectedDate, resultSet.getDate(1));
			assertEquals(expectedTimestamp, resultSet.getTimestamp(1));
		}
	}

}
