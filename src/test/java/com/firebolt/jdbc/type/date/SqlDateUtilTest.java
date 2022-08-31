package com.firebolt.jdbc.type.date;

import static java.sql.Time.valueOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeParseException;
import java.util.TimeZone;

import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.DefaultTimeZone;

@DefaultTimeZone("UTC")
class SqlDateUtilTest {

	@Test
	void shouldTransformTimestampWithNanos() {
		String timestampWithNanoSeconds = "2022-05-23 12:57:13.073456789";
		LocalDateTime localDateTime = LocalDateTime.of(2022, 5, 23, 12, 57, 13, 73456789);
		assertEquals(Timestamp.valueOf(localDateTime),
				SqlDateUtil.transformToTimestampFunction.apply(timestampWithNanoSeconds, TimeZone.getTimeZone("UTC")));
	}

	@Test
	void shouldTransformDate() {
		String timeWithNanoSeconds = "2022-05-23";
		assertEquals(Date.valueOf(LocalDate.of(2022, 5, 23)),
				SqlDateUtil.transformToDateFunction.apply(timeWithNanoSeconds, TimeZone.getTimeZone("UTC")));
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
	void shouldTransformTimestampWithNanosToString() {
		String expectedTimestampWithNanosString = "'2022-05-23 12:57:13.000173456'";
		Timestamp timestamp = Timestamp.valueOf(LocalDateTime.of(2022, 5, 23, 12, 57, 13, 173456));
		assertEquals(expectedTimestampWithNanosString,
				SqlDateUtil.transformFromTimestampToSQLStringFunction.apply(timestamp));
	}

	@Test
	void shouldTransformTimestampToString() {
		String expectedTimestamp = "'2022-05-23 12:57:13'";
		Timestamp timestamp = Timestamp.valueOf(LocalDateTime.of(2022, 5, 23, 12, 57, 13));
		assertEquals(expectedTimestamp, SqlDateUtil.transformFromTimestampToSQLStringFunction.apply(timestamp));
	}

	@Test
	void shouldTransformTime() {
		String time = "2022-05-23 12:01:13";
		Time t = valueOf(LocalTime.of(12, 1, 13));
		assertEquals(t, SqlDateUtil.transformToTimeFunction.apply(time, TimeZone.getTimeZone("UTC")));
	}

	@Test
	void shouldTransformTimeWithUTCWhenTimeZoneIsNotSpecified() {
		String time = "2022-08-23 12:01:13";
		Time t = valueOf(LocalTime.of(12, 1, 13));
		assertEquals(t, SqlDateUtil.transformToTimeFunction.apply(time, null));
	}

	@Test

	void shouldTransformTimestampWithoutSeconds() {
		String timeWithoutSeconds = "2022-05-23 12:01";
		LocalDateTime localDateTime = LocalDateTime.of(2022, 5, 23, 12, 1);
		assertEquals(Timestamp.valueOf(localDateTime),
				SqlDateUtil.transformToTimestampFunction.apply(timeWithoutSeconds, TimeZone.getTimeZone("UTC")));
	}

	@Test
	void shouldTransformTimestampToDefaultUTC() {
		String timeWithoutSeconds = "2022-05-23";
		LocalDateTime localDateTime = LocalDateTime.of(2022, 5, 23, 0, 0);
		assertEquals(Timestamp.valueOf(localDateTime),
				SqlDateUtil.transformToTimestampFunction.apply(timeWithoutSeconds, TimeZone.getTimeZone("UTC")));
	}

	@Test
	void shouldTransformTimeWithoutSeconds() {
		String timeWithoutSeconds = "2022-05-23 12:01";
		Time time = valueOf(LocalTime.of(12, 1));
		assertEquals(time, SqlDateUtil.transformToTimeFunction.apply(timeWithoutSeconds, null));
	}

	@Test
	void shouldNotTransformTimeWhenMinutesAreMissing() {
		String timeWithMissingMinutes = "2022-05-23 12";
		assertThrows(DateTimeParseException.class,
				() -> SqlDateUtil.transformToTimeFunction.apply(timeWithMissingMinutes, null));
	}

	@Test
	void shouldTransformTimestampWithUTCWhenTimeZoneIsNotSpecified() {
		String timestampWithNanoSeconds = "2022-08-23 12:57:13.073456789";
		LocalDateTime localDateTime = LocalDateTime.of(2022, 8, 23, 12, 57, 13, 73456789);
		assertEquals(Timestamp.valueOf(localDateTime),
				SqlDateUtil.transformToTimestampFunction.apply(timestampWithNanoSeconds, null));
	}

}
