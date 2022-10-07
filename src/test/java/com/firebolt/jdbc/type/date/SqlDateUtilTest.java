package com.firebolt.jdbc.type.date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.TimeZone;

import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.DefaultTimeZone;

@DefaultTimeZone("UTC")
class SqlDateUtilTest {

	private static final TimeZone UTC_TZ = TimeZone.getTimeZone("UTC");

	@Test
	void shouldTransformTimestampWithDefaultTzWhenTimeZoneIsNotSpecified() {
		String timestamp = "1000-08-23 12:57:13.073456789";
		ZonedDateTime zonedDateTime = ZonedDateTime.of(1000, 8, 23, 12, 57, 13, 73456789, UTC_TZ.toZoneId());
		Timestamp expectedTimestamp = Timestamp.valueOf(zonedDateTime.toLocalDateTime());
		assertEquals(expectedTimestamp, SqlDateUtil.transformToTimestampFunction.apply(timestamp, null));
	}

	@Test
	void shouldTransformTimestampWithNanosToString() {
		String expectedTimestamp = "'2022-05-23 12:57:13.000173456'";
		Timestamp timestamp = Timestamp
				.valueOf(ZonedDateTime.of(2022, 5, 23, 12, 57, 13, 173456, UTC_TZ.toZoneId()).toLocalDateTime());
		timestamp.setNanos(173456);
		assertEquals(expectedTimestamp, SqlDateUtil.transformFromTimestampToSQLStringFunction.apply(timestamp));
	}

	@Test
	void shouldTransformTimestampToString() {
		String expectedTimestamp = "'2022-05-23 12:57:13'";
		Timestamp timestamp = Timestamp
				.valueOf(ZonedDateTime.of(2022, 5, 23, 12, 57, 13, 0, UTC_TZ.toZoneId()).toLocalDateTime());
		assertEquals(expectedTimestamp, SqlDateUtil.transformFromTimestampToSQLStringFunction.apply(timestamp));
	}

	@Test
	void shouldTransformTimestampWithoutSeconds() {
		String timeWithoutSeconds = "2022-05-23 12:01";
		Timestamp expectedTimestamp = Timestamp
				.valueOf(ZonedDateTime.of(2022, 5, 23, 12, 1, 0, 0, UTC_TZ.toZoneId()).toLocalDateTime());
		assertEquals(expectedTimestamp, SqlDateUtil.transformToTimestampFunction.apply(timeWithoutSeconds, null));
	}

	@Test
	void shouldTransformDate() {
		String date = "1000-05-23";
		Date expectedDate = Date.valueOf(ZonedDateTime.of(1000, 5, 23, 0, 0, 0, 0, UTC_TZ.toZoneId()).toLocalDate());
		assertEquals(expectedDate, SqlDateUtil.transformToDateFunction.apply(date, null));
	}

	@Test
	void shouldTransformDateUsingTimeZoneWhenProvided() {
		String date = "2022-05-22";
		ZonedDateTime zonedDateTime = ZonedDateTime.of(2022, 5, 21, 14, 0, 0, 0, UTC_TZ.toZoneId());
		assertEquals(Date.valueOf(zonedDateTime.toLocalDate()),
				SqlDateUtil.transformToDateFunction.apply(date, TimeZone.getTimeZone("Australia/Sydney")));
	}

	@Test
	void shouldTransformTime() {
		ZonedDateTime zdt = ZonedDateTime.of(2022, 5, 23, 12, 1, 13, 0, UTC_TZ.toZoneId());
		String time = "2022-05-23 12:01:13";
		Time expectedTime = Time.valueOf(zdt.toLocalTime());

		assertEquals(expectedTime, SqlDateUtil.transformToTimeFunction.apply(time, UTC_TZ));
	}

	@Test
	void shouldTransformTimeWithUTCWhenTimeZoneIsNotSpecified() {
		String time = "2022-08-23 12:01:13";
		ZonedDateTime zdt = ZonedDateTime.of(2022, 8, 23, 12, 1, 13, 0, UTC_TZ.toZoneId());
		Time expectedTime = Time.valueOf(zdt.toLocalTime());
		assertEquals(expectedTime, SqlDateUtil.transformToTimeFunction.apply(time, null));
	}

	@Test
	void shouldTransformTimeWithoutSeconds() {
		String timeWithoutSeconds = "2022-05-23 12:01";
		ZonedDateTime zdt = ZonedDateTime.of(2022, 5, 23, 12, 1, 0, 0, UTC_TZ.toZoneId());
		Time expectedTime = Time.valueOf(zdt.toLocalTime());
		assertEquals(expectedTime, SqlDateUtil.transformToTimeFunction.apply(timeWithoutSeconds, null));
	}

	@Test
	void shouldNotTransformTimeWhenMinutesAreMissing() {
		String timeWithMissingMinutes = "2022-05-23 12";
		assertThrows(DateTimeParseException.class,
				() -> SqlDateUtil.transformToTimeFunction.apply(timeWithMissingMinutes, null));
	}

	@Test
	void shouldThrowExceptionWhenTheStringCannotBeParsedToATimestamp() {
		String timestamp = "20225-05-hey";
		assertThrows(DateTimeParseException.class,
				() -> SqlDateUtil.transformToTimestampFunction.apply(timestamp, null));
	}

	@Test
	void shouldThrowExceptionWhenTheStringCannotBeParsedToADate() {
		String date = "20225-05-hey";
		assertThrows(DateTimeParseException.class, () -> SqlDateUtil.transformToDateFunction.apply(date, null));
	}

}
