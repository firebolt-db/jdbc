package io.firebolt.jdbc.resultset.type.date;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.function.Function;

@UtilityClass
@Slf4j
public class SqlDateUtil {
  private static final String DATE_PATTERN = "yyyy-MM-dd";
  private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(DATE_PATTERN);

  private static final String DATE_TIME_PATTERN = "yyyy-MM-dd HH:mm:ss";

  private final SimpleDateFormat DATE_TIME_FORMAT = new SimpleDateFormat(DATE_TIME_PATTERN);
  private static final String DATE_TIME_WITH_MILLIS_PATTERN = "yyyy-MM-dd HH:mm:ss:SSS";

  private final SimpleDateFormat DATE_TIME_FORMAT_WITH_MILLIS =
      new SimpleDateFormat(DATE_TIME_WITH_MILLIS_PATTERN);
  public static final Function<String, Date> transformToDateFunction =
      value -> {
        if (value == null) {
          return null;
        }
        try {
          return new java.sql.Date(DATE_FORMAT.parse(value).getTime());
        } catch (ParseException e) {
          log.error(String.format("Could not parse date from String: %s", value), e);
          return null;
        }
      };
  public static final Function<String, Timestamp> transformToTimestampFunction =
      value -> {
        if (value == null) {
          return null;
        }
        try {
          return getTimestampFromString(value);
        } catch (ParseException e) {
          log.error(String.format("Could not parse timestamp from String: %s", value), e);
          return null;
        }
      };

  private static Timestamp getTimestampFromString(String value) throws ParseException {
    if (isDateTimeWithNanos(value)) {
      String dateTimeValueWithMillis =
          StringUtils.substring(value, 0, DATE_TIME_WITH_MILLIS_PATTERN.length());
      return new Timestamp(
          (new Date(DATE_TIME_FORMAT_WITH_MILLIS.parse(dateTimeValueWithMillis).getTime())
              .getTime()));
    } else {
      return new Timestamp((new Date(DATE_TIME_FORMAT.parse(value).getTime()).getTime()));
    }
  }

  private static boolean isDateTimeWithNanos(String value) {
    try {
      new Timestamp((new Date(DATE_TIME_FORMAT_WITH_MILLIS.parse(value).getTime()).getTime()));
      return true;
    } catch (ParseException ex) {
      return false;
    }
  }
}
