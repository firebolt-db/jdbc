package com.firebolt.jdbc.type.date;

import static com.firebolt.jdbc.type.date.SqlDateUtil.ONE_DAY_MILLIS;
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
		Timestamp expectedTimestamp = new Timestamp(zonedDateTime.toInstant().toEpochMilli() + ONE_DAY_MILLIS * 6 );
		expectedTimestamp.setNanos(73456789);
		assertEquals(expectedTimestamp, SqlDateUtil.transformToTimestampFunction.apply(timestamp, null));
	}

	@Test
	void shouldTransformTimestampWithNanosToString() {
		String expectedTimestamp = "'2022-05-23 12:57:13.000173456'";
		Timestamp timestamp = new Timestamp(
				ZonedDateTime.of(2022, 5, 23, 12, 57, 13, 173456, UTC_TZ.toZoneId()).toInstant().toEpochMilli());
		timestamp.setNanos(173456);
		assertEquals(expectedTimestamp, SqlDateUtil.transformFromTimestampToSQLStringFunction.apply(timestamp));
	}

	@Test
	void shouldTransformTimestampToString() {
		String expectedTimestamp = "'2022-05-23 12:57:13'";
		Timestamp timestamp = new Timestamp(
				ZonedDateTime.of(2022, 5, 23, 12, 57, 13, 0, UTC_TZ.toZoneId()).toInstant().toEpochMilli());
		assertEquals(expectedTimestamp, SqlDateUtil.transformFromTimestampToSQLStringFunction.apply(timestamp));
	}

	@Test
	void shouldTransformTimestampWithoutSeconds() {
		String timeWithoutSeconds = "2022-05-23 12:01";
		Timestamp expectedTimestamp = new Timestamp(
				ZonedDateTime.of(2022, 5, 23, 12, 1, 0, 0, UTC_TZ.toZoneId()).toInstant().toEpochMilli());
		assertEquals(expectedTimestamp, SqlDateUtil.transformToTimestampFunction.apply(timeWithoutSeconds, null));
	}

	@Test
	void shouldTransformDate() {
		String date = "1000-05-23";
		Date expectedDate = new Date(
				ZonedDateTime.of(1000, 5, 23, 0, 0, 0, 0, UTC_TZ.toZoneId()).toInstant().toEpochMilli()
						+ 6 * ONE_DAY_MILLIS);
		assertEquals(expectedDate, SqlDateUtil.transformToDateFunction.apply(date, null));
	}

	@Test
	void shouldTransformDateUsingTimeZoneWhenProvided() {
		String date = "2022-05-22";
		ZonedDateTime zonedDateTime = ZonedDateTime.of(2022, 5, 21, 14, 0, 0, 0, UTC_TZ.toZoneId());
		assertEquals(new Date(zonedDateTime.toInstant().toEpochMilli()),
				SqlDateUtil.transformToDateFunction.apply(date, TimeZone.getTimeZone("Australia/Sydney")));
	}

	@Test
	void shouldTransformTime() {
		ZonedDateTime zdt = ZonedDateTime.of(2022, 5, 23, 12, 1, 13, 0, UTC_TZ.toZoneId());
		String time = "2022-05-23 12:01:13";
		Time expectedTime = new Time(zdt.toInstant().toEpochMilli());

		assertEquals(expectedTime, SqlDateUtil.transformToTimeFunction.apply(time, UTC_TZ));
	}

	@Test
	void shouldTransformTimeWithUTCWhenTimeZoneIsNotSpecified() {
		String time = "2022-08-23 12:01:13";
		ZonedDateTime zdt = ZonedDateTime.of(2022, 8, 23, 12, 1, 13, 0, UTC_TZ.toZoneId());
		Time expectedTime = new Time(zdt.toInstant().toEpochMilli());
		assertEquals(expectedTime, SqlDateUtil.transformToTimeFunction.apply(time, null));
	}

	@Test
	void shouldTransformTimeWithoutSeconds() {
		String timeWithoutSeconds = "2022-05-23 12:01";
		ZonedDateTime zdt = ZonedDateTime.of(2022, 5, 23, 12, 1, 0, 0, UTC_TZ.toZoneId());
		Time expectedTime = new Time(zdt.toInstant().toEpochMilli());
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

	@Test
	void shouldGetJulianToGregorianDiffMillis() {
		ZonedDateTime zonedDateTime1 = ZonedDateTime.of(1582, 10, 6, 12, 57, 13, 73456789, UTC_TZ.toZoneId());
		assertEquals(0, SqlDateUtil.calculateJulianToGregorianDiffMillis(zonedDateTime1) / ONE_DAY_MILLIS);

		ZonedDateTime zonedDateTime2 = ZonedDateTime.of(1582, 10, 4, 12, 57, 13, 73456789, UTC_TZ.toZoneId());
		assertEquals(10, SqlDateUtil.calculateJulianToGregorianDiffMillis(zonedDateTime2) / ONE_DAY_MILLIS);

		ZonedDateTime zonedDateTime3 = ZonedDateTime.of(1111, 10, 5, 12, 57, 13, 73456789, UTC_TZ.toZoneId());
		assertEquals(7, SqlDateUtil.calculateJulianToGregorianDiffMillis(zonedDateTime3) / ONE_DAY_MILLIS);

		ZonedDateTime zonedDateTime4 = ZonedDateTime.of(1100, 3, 1, 12, 57, 13, 73456789, UTC_TZ.toZoneId());
		assertEquals(7, SqlDateUtil.calculateJulianToGregorianDiffMillis(zonedDateTime4) / ONE_DAY_MILLIS);

		ZonedDateTime zonedDateTime5 = ZonedDateTime.of(1100, 2, 28, 12, 57, 13, 73456789, UTC_TZ.toZoneId());
		assertEquals(6, SqlDateUtil.calculateJulianToGregorianDiffMillis(zonedDateTime5) / ONE_DAY_MILLIS);

		ZonedDateTime zonedDateTime6 = ZonedDateTime.of(1099, 1, 29, 12, 57, 13, 73456789, UTC_TZ.toZoneId());
		assertEquals(6, SqlDateUtil.calculateJulianToGregorianDiffMillis(zonedDateTime6) / ONE_DAY_MILLIS);
	}

}
