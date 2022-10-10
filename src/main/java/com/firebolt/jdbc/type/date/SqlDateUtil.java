package com.firebolt.jdbc.type.date;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Optional;
import java.util.TimeZone;
import java.util.function.BiFunction;
import java.util.function.Function;

import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@UtilityClass
@Slf4j
public class SqlDateUtil {

	private static final TimeZone DEFAULT_TZ = TimeZone.getDefault();

	public static final long ONE_DAY_MILLIS = 86400000L;

	// Number of milliseconds at the start of the introduction of the gregorian
	// calendar(1582-10-05T00:00:00Z) from the epoch of 1970-01-01T00:00:00Z
	private static final long GREGORIAN_START_DATE_IN_MILLIS = -12220156800000L;

	private static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

	public static final Function<Date, String> transformFromDateToSQLStringFunction = value -> String.format("'%s'",
			dateFormatter.format(value.toLocalDate()));
	DateTimeFormatter dateTimeFormatter = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd [HH:mm[:ss]]")
			.appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).toFormatter();
	public static final BiFunction<String, TimeZone, Timestamp> transformToTimestampFunction = (value,
			fromTimeZone) -> parse(value, fromTimeZone).map(t -> {
				Timestamp ts = new Timestamp(getEpochMilli(t));
				ts.setNanos(t.getNano());
				return ts;
			}).orElse(null);

	public static final BiFunction<String, TimeZone, Date> transformToDateFunction = (value,
			fromTimeZone) -> parse(value, fromTimeZone).map(t -> new Date(getEpochMilli(t))).orElse(null);

	public static final BiFunction<String, TimeZone, Time> transformToTimeFunction = (value,
			fromTimeZone) -> parse(value, fromTimeZone).map(t -> new Time(getEpochMilli(t))).orElse(null);
	public static final Function<Timestamp, String> transformFromTimestampToSQLStringFunction = value -> String
			.format("'%s'", dateTimeFormatter.format(value.toLocalDateTime()));

	private static Optional<ZonedDateTime> parse(String value, @Nullable TimeZone fromTimeZone) {
		if (StringUtils.isEmpty(value)) {
			return Optional.empty();
		}
		ZoneId zoneId = fromTimeZone == null ? DEFAULT_TZ.toZoneId() : fromTimeZone.toZoneId();
		try {
			return Optional.of(LocalDateTime.parse(value, dateTimeFormatter).atZone(zoneId)
					.withZoneSameInstant(DEFAULT_TZ.toZoneId()));
		} catch (DateTimeException dateTimeException) {
			LocalDate date = LocalDate.from(dateFormatter.parse(value));
			return Optional.of(LocalDateTime.of(date.getYear(), date.getMonth(), date.getDayOfMonth(), 0, 0)
					.atZone(zoneId).withZoneSameInstant(DEFAULT_TZ.toZoneId()));
		}
	}

	private static long getEpochMilli(ZonedDateTime t) {
		return t.toInstant().toEpochMilli() + calculateJulianToGregorianDiffMillis(t);
	}

	/**
	 * Calculates the difference in ms from Julian to Gregorian date for dates that
	 * are before the 5th of Oct 1582, which is before the introduction of the
	 * Gregorian Calendar
	 * 
	 * @param zdt the date
	 * @return the difference in millis
	 */
	public static long calculateJulianToGregorianDiffMillis(ZonedDateTime zdt) {
		if (zdt.toInstant().toEpochMilli() < GREGORIAN_START_DATE_IN_MILLIS) {
			int year;
			if (zdt.getMonthValue() == 1 || (zdt.getMonthValue() == 2 && zdt.getDayOfMonth() <= 28)) {
				year = zdt.getYear() - 1;
			} else {
				year = zdt.getYear();
			}
			int hundredsOfYears = year / 100;
			long daysDiff = hundredsOfYears - (hundredsOfYears / 4L) - 2L;
			return daysDiff * ONE_DAY_MILLIS;
		} else {
			return 0;
		}
	}

}
