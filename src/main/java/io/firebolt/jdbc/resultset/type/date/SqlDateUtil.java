package io.firebolt.jdbc.resultset.type.date;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.function.Function;

@UtilityClass
@Slf4j
public class SqlDateUtil {
  private static final String DATE_PATTERN = "yyyy-MM-dd";
  private static final SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_PATTERN);
  public static final Function<String, Date> transformToDateFunction =
      value -> {
        if (value == null) {
          return null;
        }
        try {
          return new java.sql.Date(dateFormat.parse(value).getTime());
        } catch (ParseException e) {
          log.error("Could not parse date", e);
          return null;
        }
      };
  private static final String DATE_TIME_PATTERN = "yyyy-MM-dd HH:mm:ss";
  private final SimpleDateFormat dateTimeFormat = new SimpleDateFormat(DATE_TIME_PATTERN);
  public static final Function<String, Timestamp> transformToTimestampFunction =
      value -> {
        if (value == null) {
          return null;
        }
        try {
          return new Timestamp((new Date(dateTimeFormat.parse(value).getTime()).getTime()));
        } catch (ParseException e) {
          log.error("Could not parse date", e);
          return null;
        }
      };
}
