package io.firebolt.jdbc.resultset.type.date;

import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.DefaultTimeZone;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeParseException;

import static java.sql.Time.valueOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SqlDateUtilTest {

  @Test
  @DefaultTimeZone("Europe/London")
  void shouldTransformTimestampWithNanos() {
    String timeWithNanoSeconds = "2022-05-23 12:57:13.073456789";
    LocalDateTime localDateTime = LocalDateTime.of(2022, 5, 23, 12, 57, 13, 73456789);
    assertEquals(
        Timestamp.valueOf(localDateTime),
        SqlDateUtil.transformToTimestampFunction.apply(timeWithNanoSeconds));
  }

  @Test
  @DefaultTimeZone("Europe/London")
  void shouldTransformDate() {
    String timeWithNanoSeconds = "2022-05-23";
    assertEquals(
        Date.valueOf(LocalDate.of(2022, 5, 23)),
        SqlDateUtil.transformToDateFunction.apply(timeWithNanoSeconds));
  }

  @Test
  @DefaultTimeZone("Europe/London")
  void shouldThrowExceptionWhenTheStringCannotBeParsedToATimestamp() {
    String timeWithNanoSeconds = "20225-05-hey";
    assertThrows(
        DateTimeParseException.class,
        () -> SqlDateUtil.transformToTimestampFunction.apply(timeWithNanoSeconds));
  }

  @Test
  @DefaultTimeZone("Europe/London")
  void shouldThrowExceptionWhenTheStringCannotBeParsedToADate() {
    String timeWithNanoSeconds = "20225-05-hey";
    assertThrows(
        DateTimeParseException.class,
        () -> SqlDateUtil.transformToDateFunction.apply(timeWithNanoSeconds));
  }

  @Test
  @DefaultTimeZone("Europe/London")
  void shouldTransformTimestampWithNanosToString() {
    String expectedTimeWithNanosString = "'2022-05-23 12:57:13.000173456'";
    Timestamp timestamp = Timestamp.valueOf(LocalDateTime.of(2022, 5, 23, 12, 57, 13, 173456));
    assertEquals(
        expectedTimeWithNanosString,
        SqlDateUtil.transformFromTimestampToSQLStringFunction.apply(timestamp));
  }

  @Test
  @DefaultTimeZone("Europe/London")
  void shouldTransformTimestampWithoutNanosToString() {
    String expectedTimeWithNanosString = "'2022-05-23 12:57:13'";
    Timestamp timestamp = Timestamp.valueOf(LocalDateTime.of(2022, 5, 23, 12, 57, 13));
    assertEquals(
        expectedTimeWithNanosString,
        SqlDateUtil.transformFromTimestampToSQLStringFunction.apply(timestamp));
  }

  @Test
  @DefaultTimeZone("Europe/London")
  void shouldTransformTime() {
    String timeWithNanoSeconds = "2022-05-23 12:01:13";
    Time t = valueOf(LocalTime.of(12, 1, 13));
    assertEquals(t, SqlDateUtil.transformToTimeFunction.apply(timeWithNanoSeconds));
  }
}
