package com.firebolt.jdbc.type.date;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.TimeZone;
import java.util.function.Function;

import com.firebolt.jdbc.CheckedBiFunction;

import lombok.CustomLog;
import lombok.experimental.UtilityClass;

@UtilityClass
@CustomLog
public class SqlDateUtil {

	public static final long ONE_DAY_MILLIS = 86400000L;
	public static final DateTimeFormatter dateTimeFormatter = new DateTimeFormatterBuilder()
			.appendValue(ChronoField.YEAR, 4).parseDefaulting(ChronoField.YEAR, 0)
			.appendPattern("[-]MM-dd [HH:mm[:ss]]").appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
			.appendPattern("[XXX]")
			.appendPattern("[X]").toFormatter();

	public static final Function<Timestamp, String> transformFromTimestampToSQLStringFunction = value -> String
			.format("'%s'", dateTimeFormatter.format(value.toLocalDateTime()));
	private static final TimeZone DEFAULT_SERVER_TZ = TimeZone.getTimeZone("UTC");
	private static final DateTimeFormatter dateFormatter = new DateTimeFormatterBuilder()
			.appendValue(ChronoField.YEAR, 4).parseDefaulting(ChronoField.YEAR, 0).appendPattern("[-]MM-dd")
			.toFormatter();
	public static final Function<Date, String> transformFromDateToSQLStringFunction = value -> String.format("'%s'",
			dateFormatter.format(value.toLocalDate()));
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
