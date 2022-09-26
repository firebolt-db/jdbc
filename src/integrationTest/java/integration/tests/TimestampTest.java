package integration.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import org.junit.jupiter.api.Test;

import integration.IntegrationTest;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class TimestampTest extends IntegrationTest {

	@Test
	void shouldGetTimeObjectsInEstTimezone() throws SQLException {
		try (Connection connection = this.createConnection();
				Statement statement = connection.createStatement();
				ResultSet resultSet = statement
						.executeQuery("SELECT TO_TIMESTAMP_EXT('1975/01/01 23:01:01', 6, 'EST');")) {
			resultSet.next();
			Timestamp expectedTimestamp = Timestamp.valueOf(LocalDateTime.of(1975, 1, 2, 4, 1, 1));
			Time expectedTime = Time.valueOf(LocalTime.of(4, 1, 1));
			Date expectedDate = Date.valueOf(LocalDate.of(1975, 1, 2));
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
			Timestamp expectedTimestamp = Timestamp.valueOf(LocalDateTime.of(1975, 1, 1, 23, 1, 1));
			Time expectedTime = Time.valueOf(LocalTime.of(23, 1, 1));
			Date expectedDate = Date.valueOf(LocalDate.of(1975, 1, 1));
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
			Timestamp expectedTimestamp = Timestamp.valueOf(LocalDateTime.of(1111, 11, 11, 12, 0, 3));
			Time expectedTime = Time.valueOf(LocalTime.of(12, 0, 3));
			Date expectedDate = Date.valueOf(LocalDate.of(1111, 11, 11));
			assertEquals(expectedTimestamp, resultSet.getTimestamp(1));
			assertEquals(expectedTimestamp, resultSet.getObject(1));
			assertEquals(expectedTime, resultSet.getTime(1));
			assertEquals(expectedDate, resultSet.getDate(1));
			assertEquals(expectedTimestamp, resultSet.getTimestamp(1));
		}
	}

}
