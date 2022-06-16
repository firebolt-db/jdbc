package io.firebolt.jdbc.resultset.type.date;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.sql.Time;
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

  public static final Function<Date, String> transformFromDateFunction =
      value -> {
        if (value == null) {
          return null;
        }
        return String.format("'%s'", DATE_FORMAT.format(value));
      };

  public static final Function<Time, String> transformFromTimeFunction =
      value -> {
        if (value == null) {
          return null;
        }
        return String.format("'%s'", DATE_TIME_FORMAT.format(value));
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
      return getTimestampWithNanos(value);
    } else {
      return new Timestamp((new Date(DATE_TIME_FORMAT.parse(value).getTime()).getTime()));
    }
  }

  /*
  SimpleDateFormat does not support nanoseconds, so we need to extract the nanos and then set the timestamp
   */
  private static Timestamp getTimestampWithNanos(String value) throws ParseException {
    String dateTimeWithoutNanos = StringUtils.substring(value, 0, DATE_TIME_PATTERN.length());
    Timestamp timestamp =
        new Timestamp((new Date(DATE_TIME_FORMAT.parse(dateTimeWithoutNanos).getTime()).getTime()));
    int nanos = extractNanos(value, dateTimeWithoutNanos);
    timestamp.setNanos(nanos);
    return timestamp;
  }

  private static int extractNanos(String dateTimeWithNanos, String dateTimeWithoutNanos) {
    if (dateTimeWithNanos.length() != dateTimeWithoutNanos.length()) {
      StringBuilder nanosPart =
          new StringBuilder(dateTimeWithNanos.substring(dateTimeWithoutNanos.length() + 1));
      while (nanosPart.length() < 9) {
        nanosPart.append("0");
      }
      return Integer.parseInt(nanosPart.toString());
    } else {
      return 0;
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

  public static final Function<Timestamp, String> transformFromTimestampFunction =
      value -> {
        if (value == null) {
          return null;
        }
        if (value.getNanos() > 0) {
          long nanos = value.getNanos();
          value.setNanos(0);
          StringBuilder dateWithoutNanos =
              new StringBuilder(String.format("'%s", DATE_TIME_FORMAT.format(value)));
          return dateWithoutNanos.append(":").append(String.format("%09d", nanos)).append("'").toString();
        } else {
          return String.format("'%s'", DATE_TIME_FORMAT.format(value));
        }
      };
}
