package com.firebolt.jdbc.type.date;

import static com.firebolt.jdbc.type.date.SqlDateUtil.ONE_DAY_MILLIS;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.*;
import java.util.TimeZone;

import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.DefaultTimeZone;

import com.firebolt.jdbc.exception.FireboltException;

@DefaultTimeZone("UTC")
class SqlDateUtilTest {

	private static final TimeZone UTC_TZ = TimeZone.getTimeZone("UTC");
	private static final TimeZone EST_TZ = TimeZone.getTimeZone("EST");

	@Test
	void shouldTransformTimestampWithDefaultTzWhenTimeZoneIsNotSpecified() throws SQLException {
		String timestamp = "1000-08-23 12:57:13.073456789";
		ZonedDateTime zonedDateTime = ZonedDateTime.of(1000, 8, 23, 12, 57, 13, 0, UTC_TZ.toZoneId());
		Timestamp expectedTimestamp = new Timestamp(zonedDateTime.toInstant().toEpochMilli() + ONE_DAY_MILLIS * 6);
		expectedTimestamp.setNanos(73456789);
		assertEquals(expectedTimestamp, SqlDateUtil.transformToTimestampFunction.apply(timestamp, null));
	}

	@Test
	void shouldTransformTimestampWithNanosToString() throws SQLException {
		String expectedTimestamp = "'2022-05-23 12:57:13.000173456'";
		Timestamp timestamp = new Timestamp(
				ZonedDateTime.of(2022, 5, 23, 12, 57, 13, 0, UTC_TZ.toZoneId()).toInstant().toEpochMilli());
		timestamp.setNanos(173456);
		assertEquals(expectedTimestamp, SqlDateUtil.transformFromTimestampToSQLStringFunction.apply(timestamp));
	}

	@Test
	void shouldTransformTimestampToString() throws SQLException {
		String expectedTimestamp = "'2022-05-23 12:57:13'";
		Timestamp timestamp = new Timestamp(
				ZonedDateTime.of(2022, 5, 23, 12, 57, 13, 0, UTC_TZ.toZoneId()).toInstant().toEpochMilli());
		assertEquals(expectedTimestamp, SqlDateUtil.transformFromTimestampToSQLStringFunction.apply(timestamp));
	}

	@Test
	void shouldTransformDate() throws SQLException {
		String date = "1000-05-23";
		Date expectedDate = new Date(
				ZonedDateTime.of(1000, 5, 23, 0, 0, 0, 0, UTC_TZ.toZoneId()).toInstant().toEpochMilli()
						+ 6 * ONE_DAY_MILLIS);
		assertEquals(expectedDate, SqlDateUtil.transformToDateFunction.apply(date, null));
	}

	@Test
	void shouldTransformDateUsingTimeZoneWhenProvided() throws SQLException {
		String date = "2022-05-22";
		ZonedDateTime zonedDateTime = ZonedDateTime.of(2022, 5, 21, 14, 0, 0, 0, UTC_TZ.toZoneId());
		assertEquals(new Date(zonedDateTime.toInstant().toEpochMilli()),
				SqlDateUtil.transformToDateFunction.apply(date, TimeZone.getTimeZone("Australia/Sydney")));
	}

	@Test
	void shouldTransformTime() throws SQLException {
		String time = "2022-05-23 23:01:13";
		ZonedDateTime zdt = ZonedDateTime.of(1970, 1, 2, 4, 1, 13, 0, UTC_TZ.toZoneId());
		Time expectedTime = new Time(zdt.toInstant().toEpochMilli());

		assertEquals(expectedTime, SqlDateUtil.transformToTimeFunction.apply(time, EST_TZ));
	}

	@Test
	void shouldTransformTimeWithUTCWhenTimeZoneIsNotSpecified() throws SQLException {
		String time = "2022-08-23 12:01:13";
		ZonedDateTime zdt = ZonedDateTime.of(1970, 1, 1, 12, 1, 13, 0, UTC_TZ.toZoneId());
		Time expectedTime = new Time(zdt.toInstant().toEpochMilli());
		assertEquals(expectedTime, SqlDateUtil.transformToTimeFunction.apply(time, null));
	}

	@Test
	void shouldThrowExceptionWhenParsingInvalidDateTimeWithoutSeconds() throws SQLException {
		String timeWithoutSeconds = "2022-05-23 12:01";
		assertThrows(FireboltException.class,
				() -> SqlDateUtil.transformToTimeFunction.apply(timeWithoutSeconds, null));
	}

	@Test
	void shouldNotTransformTimeWhenMinutesAreMissing() throws SQLException {
		String timeWithMissingMinutes = "2022-05-23 12";
		assertThrows(FireboltException.class,
				() -> SqlDateUtil.transformToTimeFunction.apply(timeWithMissingMinutes, null));
	}

	@Test
	void shouldThrowExceptionWhenTheStringCannotBeParsedToATimestamp() throws SQLException {
		String timestamp = "20225-05-hey";
		assertThrows(FireboltException.class,
				() -> SqlDateUtil.transformToTimestampFunction.apply(timestamp, null));
	}

	@Test
	void shouldThrowExceptionWhenTheStringCannotBeParsedToADate() throws SQLException {
		String date = "20225-05-hey";
		assertThrows(FireboltException.class, () -> SqlDateUtil.transformToDateFunction.apply(date, null));
	}

	@Test
	void shouldTransformDateWithZeroYear() throws SQLException {
		String timeWithMissingMinutes = "0000-01-01";
		Date date = Date.valueOf(LocalDate.of(0, 1, 1));
		assertEquals(date, SqlDateUtil.transformToDateFunction.apply(timeWithMissingMinutes, null));
	}

	@Test
	void shouldTransformTimestampDateWithZeroYear() throws SQLException {
		String timeWithMissingMinutes = "0000-01-01 12:13:14";
		Timestamp ts = Timestamp.valueOf(LocalDateTime.of(0, 1, 1, 12, 13, 14));
		assertEquals(ts, SqlDateUtil.transformToTimestampFunction.apply(timeWithMissingMinutes, null));
	}

	@Test
	void shouldTransformTimestampTz() throws SQLException {
		String timeWithTimezone = "2023-01-05 16:04:42.123456+00";
		Timestamp expectedTimestamp = new Timestamp(
				ZonedDateTime.of(2023, 01, 05, 16, 4, 42, 0, UTC_TZ.toZoneId()).toInstant().toEpochMilli());
		expectedTimestamp.setNanos(123456000);
		assertEquals(expectedTimestamp, SqlDateUtil.transformToTimestampFunction.apply(timeWithTimezone, null));
	}

	@Test
	void shouldTransformTimestampTzWithDifferentFormatTz() throws SQLException {
		String timeWithTimezone = "2023-01-05 16:04:42.123456+05:30";
		Timestamp expectedTimestamp = new Timestamp(
				ZonedDateTime.of(2023, 1, 5, 10, 34, 42, 0, UTC_TZ.toZoneId()).toInstant().toEpochMilli());
		expectedTimestamp.setNanos(123456000);
		assertEquals(expectedTimestamp, SqlDateUtil.transformToTimestampFunction.apply(timeWithTimezone, null));
	}

	@Test
	void shouldTransformTimestampTzWithDifferentFormatTzWithSeconds() throws SQLException {
		String timeWithTimezone = "2023-01-05 16:04:42.123456+05:30:30";
		Timestamp expectedTimestamp = new Timestamp(
				ZonedDateTime.of(2023, 1, 5, 10, 34, 12, 0, UTC_TZ.toZoneId()).toInstant().toEpochMilli());
		expectedTimestamp.setNanos(123456000);
		assertEquals(expectedTimestamp, SqlDateUtil.transformToTimestampFunction.apply(timeWithTimezone, null));
	}

	@Test
	void shouldTransformTimestampTzWithoutTz() throws SQLException {
		String timeWithTimezone = "2023-01-05 17:04:42.123456";
		Timestamp expectedTimestamp = new Timestamp(
				ZonedDateTime.of(2023, 1, 5, 17, 4, 42, 0, UTC_TZ.toZoneId()).toInstant().toEpochMilli());
		expectedTimestamp.setNanos(123456000);
		assertEquals(expectedTimestamp, SqlDateUtil.transformToTimestampFunction.apply(timeWithTimezone, null));
	}

	@Test
	void shouldTransformTimestamptzToDate() throws SQLException {
		String date = "2022-05-10 21:01:02-05";
		ZonedDateTime zonedDateTime = ZonedDateTime.of(2022, 5, 11, 0, 0, 0, 0, UTC_TZ.toZoneId());
		assertEquals(new Date(zonedDateTime.toInstant().toEpochMilli()),
				SqlDateUtil.transformToDateFunction.apply(date, null));
	}

	@Test
	void shouldTransformTimestampntzToDate() throws SQLException {
		String date = "2022-05-10 21:01:02";
		ZonedDateTime zonedDateTime = ZonedDateTime.of(2022, 5, 10, 0, 0, 0, 0, UTC_TZ.toZoneId());
		assertEquals(new Date(zonedDateTime.toInstant().toEpochMilli()),
				SqlDateUtil.transformToDateFunction.apply(date, null));
	}

	@Test
	void shouldTransformTimestamptzToTimestamp() throws SQLException {
		String date = "2022-05-10 21:01:02-05";
		Timestamp expectedTimestamp = new Timestamp(
				ZonedDateTime.of(2022, 5, 11, 2, 1, 2, 0, UTC_TZ.toZoneId()).toInstant().toEpochMilli());
		expectedTimestamp.setNanos(123);
		assertEquals(new Timestamp(expectedTimestamp.toInstant().toEpochMilli()),
				SqlDateUtil.transformToTimestampFunction.apply(date, null));
		// The tz remains the same when the tz info is already is part of the response
		assertEquals(new Timestamp(expectedTimestamp.toInstant().toEpochMilli()),
				SqlDateUtil.transformToTimestampFunction.apply(date, EST_TZ));
	}

	@Test
	void shouldTransformTimestampntzToTimestamp() throws SQLException {
		String date = "2022-05-10 23:01:02.0";
		Timestamp expectedTimestamp = new Timestamp(
				ZonedDateTime.of(2022, 5, 10, 23, 1, 2, 0, UTC_TZ.toZoneId()).toInstant().toEpochMilli());
		assertEquals(new Timestamp(expectedTimestamp.toInstant().toEpochMilli()),
				SqlDateUtil.transformToTimestampFunction.apply(date, null));

		Timestamp expectedTimestampWithDifferentTz = new Timestamp(
				ZonedDateTime.of(2022, 5, 11, 4, 1, 2, 0, UTC_TZ.toZoneId()).toInstant().toEpochMilli());
		assertEquals(new Timestamp(expectedTimestampWithDifferentTz.toInstant().toEpochMilli()),
				SqlDateUtil.transformToTimestampFunction.apply(date, EST_TZ));
	}

	@Test
	void shouldTransformTimestampToOffsetDateTime() throws SQLException {
		Timestamp timestamp = new Timestamp(
				ZonedDateTime.of(2022, 5, 10, 23, 1, 2, 0, UTC_TZ.toZoneId()).toInstant().toEpochMilli());
		OffsetDateTime expectedOffsetDateTime = OffsetDateTime.of(timestamp.toLocalDateTime(), ZoneOffset.of("+00:00"));
		assertEquals(expectedOffsetDateTime, SqlDateUtil.transformFromTimestampToOffsetDateTime.apply(timestamp));
	}

	@Test
	void shouldTransformTimestampToNullOffsetDateTimeWhenTimestampIsNull() throws SQLException {
		assertNull(SqlDateUtil.transformFromTimestampToOffsetDateTime.apply(null));
	}

	@Test
	@DefaultTimeZone("Asia/Kolkata")
	void shouldTransformTimestampWithoutOffsetDifference() throws SQLException {
		// The tz offset was different in 1899 (+05:21:10) - compared to +05:30 today
		String dateTime = "1899-01-01 00:00:00";
		long offsetDiffInMillis = ((8 * 60) + 50) * 1000L; // 8:50 in millis
		ZonedDateTime expectedTimestampZdt = ZonedDateTime.of(1899, 1, 1, 0, 0, 0, 0,
				TimeZone.getTimeZone("Asia/Kolkata").toZoneId());
		Timestamp expectedTimestamp = new Timestamp(
				expectedTimestampZdt.toInstant().toEpochMilli() - offsetDiffInMillis);
		assertEquals(expectedTimestamp, SqlDateUtil.transformToTimestampFunction.apply(dateTime, null));
	}
}
