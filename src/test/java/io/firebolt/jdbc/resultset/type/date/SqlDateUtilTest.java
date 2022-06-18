package io.firebolt.jdbc.resultset.type.date;

import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.DefaultTimeZone;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SqlDateUtilTest {

  @Test
  @DefaultTimeZone("Europe/London")
  void shouldTransformTimestampWithNanos() {
    String timeWithNanoSeconds = "2022-05-23 12:57:13.173456789";
    LocalDateTime localDateTime = LocalDateTime.of(2022, 5, 23, 12, 57, 13, 173456789);
    assertEquals(
        Timestamp.valueOf(localDateTime),
        SqlDateUtil.transformToTimestampFunction.apply(timeWithNanoSeconds));
  }

  @Test
  @DefaultTimeZone("Europe/London")
  void shouldTransformTimestampWithMillis() {
    String timeWithNanoSeconds = "2022-05-23 12:57:13.173";
    assertEquals(
        new Timestamp(1653307033173L),
        SqlDateUtil.transformToTimestampFunction.apply(timeWithNanoSeconds));
  }

  @Test
  @DefaultTimeZone("Europe/London")
  void shouldTransformTimestampWithoutNanos() {
    String timeWithNanoSeconds = "2022-05-23 12:57:13";
    assertEquals(
        new Timestamp(1653307033000L),
        SqlDateUtil.transformToTimestampFunction.apply(timeWithNanoSeconds));
  }

  @Test
  @DefaultTimeZone("Europe/London")
  void shouldTransformTimestampWithNanos2() {
    String timeWithNanoSeconds = "2004-07-09 10:17:35.001000";
    assertEquals(Timestamp.valueOf(LocalDateTime.of(2004,7,9,10,17,35,1000000)),
            SqlDateUtil.transformToTimestampFunction.apply(timeWithNanoSeconds));
  }

  @Test
  @DefaultTimeZone("Europe/London")
  void shouldTransformDate() {
    String timeWithNanoSeconds = "2022-05-23";
    assertEquals(
        new Date(1653260400000L), SqlDateUtil.transformToDateFunction.apply(timeWithNanoSeconds));
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
    String expectedTimeWithNanosString = "'2022-05-23 12:57:13.173456789'";
    Timestamp timestamp = new Timestamp(1653307033173L);
    timestamp.setNanos(173456789);
    assertEquals(
        expectedTimeWithNanosString, SqlDateUtil.transformFromTimestampFunction.apply(timestamp));
  }

  @Test
  @DefaultTimeZone("Europe/London")
  void shouldTransformTimestampWithSomeNanosToString() {
    String expectedTimeWithNanosString = "'2022-05-23 12:57:13.000173456'";
    Timestamp timestamp = new Timestamp(1653307033173L);
    timestamp.setNanos(173456);
    assertEquals(
        expectedTimeWithNanosString, SqlDateUtil.transformFromTimestampFunction.apply(timestamp));
  }

  @Test
  @DefaultTimeZone("Europe/London")
  void shouldTransformTimestampWithoutNanosToString() {
    String expectedTimeWithNanosString = "'2022-05-23 12:57:13'";
    Timestamp timestamp = new Timestamp(1653307033000L);
    assertEquals(
        expectedTimeWithNanosString, SqlDateUtil.transformFromTimestampFunction.apply(timestamp));
  }
}
