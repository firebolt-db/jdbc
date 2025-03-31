package com.firebolt.jdbc.type.date;

import com.firebolt.jdbc.CheckedBiFunction;
import lombok.CustomLog;
import lombok.experimental.UtilityClass;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.TimeZone;
import java.util.function.BiFunction;
import java.util.function.Function;

@UtilityClass
@CustomLog
public class SqlDateUtil {

	public static final long ONE_DAY_MILLIS = 86400000L;
	public static final DateTimeFormatter dateTimeFormatter = new DateTimeFormatterBuilder()
			.appendValue(ChronoField.YEAR, 4).parseDefaulting(ChronoField.YEAR, 0)
			.appendPattern("[-]MM-dd [HH:mm[:ss]]").appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
			.appendPattern("[XXX]")
			.appendPattern("[X]").toFormatter();

	public static final Function<LocalDateTime, String> transformFromLocalDateTimeToSQLStringFunction = value -> String
			.format("'%s'", dateTimeFormatter.format(value));
	public static final Function<OffsetDateTime, String> transformFromOffsetDateTimeToSQLStringFunction = value -> String
			.format("'%s'", dateTimeFormatter.format(value));
	public static final Function<Timestamp, String> transformFromTimestampToSQLStringFunction = value ->
			transformFromLocalDateTimeToSQLStringFunction.apply(value.toLocalDateTime());
	public static final BiFunction<Timestamp, TimeZone, String> transformFromTimestampWithTimezoneToSQLStringFunction = (ts, tz) -> String
			.format("'%s'", dateTimeFormatter.format(ts.toInstant().atZone(tz.toZoneId()).toLocalDateTime()));
	private static final TimeZone DEFAULT_SERVER_TZ = TimeZone.getTimeZone("UTC");
	private static final DateTimeFormatter dateFormatter = new DateTimeFormatterBuilder()
			.appendValue(ChronoField.YEAR, 4).parseDefaulting(ChronoField.YEAR, 0).appendPattern("[-]MM-dd")
			.toFormatter();
	public static final Function<LocalDate, String> transformFromLocalDateToSQLStringFunction = value -> String.format("'%s'",
			dateFormatter.format(value));
	public static final Function<Date, String> transformFromDateToSQLStringFunction = value -> transformFromLocalDateToSQLStringFunction.apply(value.toLocalDate());
	public static final BiFunction<Date, TimeZone, String> transformFromDateWithTimezoneToSQLStringFunction = (date, tz) -> String.format("'%s'",
			dateFormatter.format(Instant.ofEpochMilli(date.getTime()).atZone(tz.toZoneId()).toLocalDateTime()));
	public static final CheckedBiFunction<String, TimeZone, Timestamp> transformToTimestampFunction = TimestampUtil::toTimestamp;

	public static final Function<Timestamp, OffsetDateTime> transformFromTimestampToOffsetDateTime = timestamp -> {
		if (timestamp == null) {
			return null;
		}
		OffsetDateTime offsetDateTime = OffsetDateTime.now(DEFAULT_SERVER_TZ.toZoneId());
		return timestamp.toLocalDateTime().atOffset(offsetDateTime.getOffset());
	};

	public static final CheckedBiFunction<String, TimeZone, Date> transformToDateFunction = (value,
			fromTimeZone) -> TimestampUtil.convertToDate(TimestampUtil.toTimestamp(value, fromTimeZone).getTime(), fromTimeZone);

	public static final CheckedBiFunction<String, TimeZone, Time> transformToTimeFunction = TimestampUtil::toTime;
}
