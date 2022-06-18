package io.firebolt.jdbc.resultset.type.date;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.function.Function;

@UtilityClass
@Slf4j
public class SqlDateUtil {

  DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
  DateTimeFormatter dateTimeFormatter =
      new DateTimeFormatterBuilder()
          .appendPattern("yyyy-MM-dd HH:mm:ss")
          .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
          .toFormatter();

  public static final Function<String, Date> transformToDateFunction =
      value -> {
        if (StringUtils.isEmpty(value)) {
          return null;
        }
          Date date = Date.valueOf(LocalDate.from(dateFormatter.parse(value)));
          log.debug("Converted date from {} to {}", value, date);
          return date;
      };

  public static final Function<Date, String> transformFromDateFunction =
      value -> {
        if (value == null) {
          return null;
        }
        return String.format("'%s'", dateFormatter.format(value.toLocalDate()));
      };

  public static final Function<Time, String> transformFromTimeFunction =
      value -> {
        if (value == null) {
          return null;
        }
        return String.format("'%s'", dateTimeFormatter.format(value.toLocalTime()));
      };

  public static final Function<String, java.sql.Timestamp> transformToTimestampFunction =
      value -> {
        if (StringUtils.isEmpty(value)) {
          return null;
        }
        Timestamp ts =
            java.sql.Timestamp.valueOf(LocalDateTime.from(dateTimeFormatter.parse(value)));
        log.debug("Converted timestamp from {} to {}", value, ts);
        return ts;
      };

  public static final Function<String, Time> transformToTimeFunction =
      value -> {
        if (StringUtils.isEmpty(value)) {
          return null;
        }
        Time time = Time.valueOf(LocalTime.from(dateTimeFormatter.parse(value)));
        log.debug("Converted time from {} to {}", value, time);
        return time;
      };

  public static final Function<Timestamp, String> transformFromTimestampFunction =
      value -> {
        if (value == null) {
          return null;
        }
        return String.format("'%s'", dateTimeFormatter.format(value.toLocalDateTime()));
      };
}
