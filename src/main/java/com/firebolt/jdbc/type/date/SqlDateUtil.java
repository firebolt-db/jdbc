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

	private static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

	public static final Function<Date, String> transformFromDateToSQLStringFunction = value -> String.format("'%s'",
			dateFormatter.format(value.toLocalDate()));
	DateTimeFormatter dateTimeFormatter = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd [HH:mm[:ss]]")
			.appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).toFormatter();
	public static final BiFunction<String, TimeZone, Timestamp> transformToTimestampFunction = (value,
			fromTimeZone) -> parse(value, fromTimeZone).map(t -> Timestamp.valueOf(t.toLocalDateTime())).orElse(null);

	public static final BiFunction<String, TimeZone, Date> transformToDateFunction = (value,
			fromTimeZone) -> parse(value, fromTimeZone).map(t -> Date.valueOf(t.toLocalDate())).orElse(null);

	public static final BiFunction<String, TimeZone, Time> transformToTimeFunction = (value,
			fromTimeZone) -> parse(value, fromTimeZone).map(t -> Time.valueOf(t.toLocalTime())).orElse(null);
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
}
