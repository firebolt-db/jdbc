package io.firebolt.jdbc.resultset.type.date;

import org.junit.jupiter.api.Test;

import java.sql.Date;
import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class SqlDateUtilTest {

  @Test
  void shouldTransformTimestampWithNanos() {
    String timeWithNanoSeconds = "2022-05-23 12:57:13:173456789";
    Timestamp expectedTimestamp = new Timestamp(1653307033173L);
    expectedTimestamp.setNanos(173456789);
    assertEquals(
        expectedTimestamp, SqlDateUtil.transformToTimestampFunction.apply(timeWithNanoSeconds));
  }

  @Test
  void shouldTransformTimestampWithMillis() {
    String timeWithNanoSeconds = "2022-05-23 12:57:13:173";
    assertEquals(
        new Timestamp(1653307033173L),
        SqlDateUtil.transformToTimestampFunction.apply(timeWithNanoSeconds));
  }

  @Test
  void shouldTransformTimestampWithoutNanos() {
    String timeWithNanoSeconds = "2022-05-23 12:57:13";
    assertEquals(
        new Timestamp(1653307033000L),
        SqlDateUtil.transformToTimestampFunction.apply(timeWithNanoSeconds));
  }

  @Test
  void shouldTransformDate() {
    String timeWithNanoSeconds = "2022-05-23";
    assertEquals(
        new Date(1653260400000L), SqlDateUtil.transformToDateFunction.apply(timeWithNanoSeconds));
  }

  @Test
  void shouldReturnNullWhenTheStringCannotBeParsedToATimestamp() {
    String timeWithNanoSeconds = "20225-05-hey";
    assertNull(SqlDateUtil.transformToTimestampFunction.apply(timeWithNanoSeconds));
  }

  @Test
  void shouldReturnNullWhenTheStringCannotBeParsedToADate() {
    String timeWithNanoSeconds = "20225-05-hey";
    assertNull(SqlDateUtil.transformToDateFunction.apply(timeWithNanoSeconds));
  }

  @Test
  void shouldTransformTimestampWithNanosToString() {
    String expectedTimeWithNanosString = "'2022-05-23 12:57:13:017345678'";
    Timestamp timestamp = new Timestamp(1653307033173L);
    timestamp.setNanos(17345678);
    assertEquals(
        expectedTimeWithNanosString, SqlDateUtil.transformFromTimestampFunction.apply(timestamp));
  }

  @Test
  void shouldTransformTimestampWithSomeNanosToString() {
    String expectedTimeWithNanosString = "'2022-05-23 12:57:13:173456'";
    Timestamp timestamp = new Timestamp(1653307033173L);
    timestamp.setNanos(173456);
    assertEquals(
        expectedTimeWithNanosString, SqlDateUtil.transformFromTimestampFunction.apply(timestamp));
  }

  @Test
  void shouldTransformTimestampWithoutNanosToString() {
    String expectedTimeWithNanosString = "'2022-05-23 12:57:13'";
    Timestamp timestamp = new Timestamp(1653307033000L);
    assertEquals(
        expectedTimeWithNanosString, SqlDateUtil.transformFromTimestampFunction.apply(timestamp));
  }
}
