package com.firebolt.jdbc.type.date;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.TimeZone;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;

import lombok.CustomLog;
import lombok.experimental.UtilityClass;

@UtilityClass
@CustomLog
public class SqlDateUtil {

	private enum dateTimeType {
		TIME, DATE, TIMESTAMP
	}

	public static final long ONE_DAY_MILLIS = 86400000L;
	public static final DateTimeFormatter dateTimeFormatter = new DateTimeFormatterBuilder()
			.appendValue(ChronoField.YEAR, 4).parseDefaulting(ChronoField.YEAR, 0)
			.appendPattern("[-]MM-dd [HH:mm[:ss]]").appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
			.appendPattern("[XXX]")
			.appendPattern("[X]").toFormatter();

	private static final Pattern timezonePattern = Pattern.compile("([+-])([0-2]\\d$)|([+-])([0-2]\\d:\\d\\d$)|([+-])([0-2]\\d:\\d\\d:\\d\\d$)");

	public static final Function<Timestamp, String> transformFromTimestampToSQLStringFunction = value -> String
			.format("'%s'", dateTimeFormatter.format(value.toLocalDateTime()));
	private static final TimeZone DEFAULT_TZ = TimeZone.getDefault();
	// Number of milliseconds at the start of the introduction of the gregorian
	// calendar(1582-10-05T00:00:00Z) from the epoch of 1970-01-01T00:00:00Z
	private static final long GREGORIAN_START_DATE_IN_MILLIS = -12220156800000L;
	private static final DateTimeFormatter dateFormatter = new DateTimeFormatterBuilder()
			.appendValue(ChronoField.YEAR, 4).parseDefaulting(ChronoField.YEAR, 0).appendPattern("[-]MM-dd")
			.toFormatter();
	public static final Function<Date, String> transformFromDateToSQLStringFunction = value -> String.format("'%s'",
			dateFormatter.format(value.toLocalDate()));
	public static final BiFunction<String, TimeZone, Timestamp> transformToTimestampFunction = (value,
			fromTimeZone) -> parse(value, fromTimeZone, dateTimeType.TIMESTAMP).map(t -> {
				Timestamp ts = new Timestamp(getEpochMilli(t));
				ts.setNanos(t.getNano());
				return ts;
			}).orElse(null);
	public static final BiFunction<String, TimeZone, Date> transformToDateFunction = (value,
			fromTimeZone) -> parse(value, fromTimeZone, dateTimeType.DATE).map(t -> new Date(getEpochMilli(t)))
					.orElse(null);
	public static final BiFunction<String, TimeZone, Time> transformToTimeFunction = (value,
			fromTimeZone) -> parse(value, fromTimeZone, dateTimeType.TIME).map(t -> new Time(getEpochMilli(t)))
					.orElse(null);

	private static Optional<ZonedDateTime> parse(String value, @Nullable TimeZone fromTimeZone,
			dateTimeType dateTimeType) {
		if (StringUtils.isEmpty(value)) {
			return Optional.empty();
		}
		ZoneId zoneId = fromTimeZone == null ? DEFAULT_TZ.toZoneId() : fromTimeZone.toZoneId();
		try {
			ZonedDateTime zdt;
			boolean timestampContainsTz = timestampValueContainsTz(value);
			if (timestampContainsTz) {
				zdt = ZonedDateTime.parse(value, dateTimeFormatter);
			} else {
				zdt = LocalDateTime.parse(value, dateTimeFormatter).atZone(zoneId);
			}
			if (dateTimeType == SqlDateUtil.dateTimeType.DATE) {
				zdt = truncateToDate(fromTimeZone, zdt);
			} else if (dateTimeType == SqlDateUtil.dateTimeType.TIME) {
				zdt = truncateToTime(timestampContainsTz, fromTimeZone, zdt);
			}
			return Optional.of(zdt.withZoneSameInstant(DEFAULT_TZ.toZoneId()));
		} catch (DateTimeException dateTimeException) {
			LocalDateTime localDateTime;
			LocalDate date = LocalDate.from(dateFormatter.parse(value));
			if (dateTimeType == SqlDateUtil.dateTimeType.TIME) {
				localDateTime = LocalDateTime.of(1970, 1, 1, 0, 0);
			} else {
				localDateTime = LocalDateTime.of(date.getYear(), date.getMonth(), date.getDayOfMonth(), 0, 0);
			}
			return Optional.of(localDateTime.atZone(zoneId).withZoneSameInstant(DEFAULT_TZ.toZoneId()));
		}
	}

	private static ZonedDateTime truncateToTime(boolean timestampContainsTz, TimeZone fromTimeZone, ZonedDateTime zdt) {
		zdt = zdt.withDayOfMonth(1).withYear(1970).withMonth(1);
		// We only convert based on the provided tz if the value is not stored in the db
		if (fromTimeZone != null && !timestampContainsTz) {
			zdt = zdt.withZoneSameLocal(fromTimeZone.toZoneId());
		} else {
			zdt = zdt.withZoneSameInstant(DEFAULT_TZ.toZoneId());
		}
		return zdt;
	}

	private static ZonedDateTime truncateToDate(TimeZone fromTimeZone, ZonedDateTime zdt) {
		ZoneId zoneId = fromTimeZone != null ? fromTimeZone.toZoneId() : DEFAULT_TZ.toZoneId();
		return zdt.withZoneSameInstant(zoneId).truncatedTo(ChronoUnit.DAYS);
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

	private static boolean timestampValueContainsTz(String value) {
		Matcher timezoneMatcher = timezonePattern.matcher(value);
		return timezoneMatcher.find();
	}
}
