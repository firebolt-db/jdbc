package com.firebolt.jdbc.resultset.type;

import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.resultset.FireboltColumn;
import com.firebolt.jdbc.resultset.type.array.FireboltArray;
import com.firebolt.jdbc.resultset.type.array.SqlArrayUtil;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.DefaultTimeZone;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.firebolt.jdbc.exception.ExceptionType.TYPE_NOT_SUPPORTED;
import static com.firebolt.jdbc.exception.ExceptionType.TYPE_TRANSFORMATION_ERROR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JavaTypeToFireboltSQLStringTest {

  @Test
  void shouldTransformNullToString() throws FireboltException {
    assertEquals("\\N", JavaTypeToFireboltSQLString.transformAny(null));
  }

  @Test
  void shouldTransformBooleanToString() throws FireboltException {
    assertEquals("1", JavaTypeToFireboltSQLString.BOOLEAN.transform(true));

    assertEquals("0", JavaTypeToFireboltSQLString.BOOLEAN.transform(false));

    assertEquals("\\N", JavaTypeToFireboltSQLString.BOOLEAN.transform(null));
  }

  @Test
  void shouldTransformUUIDToString() throws FireboltException {
    String uuidValue = "2ac03dc8-f7c9-11ec-b939-0242ac120002";
    UUID uuid = UUID.fromString(uuidValue);
    assertEquals(uuidValue, JavaTypeToFireboltSQLString.UUID.transform(uuid));

    assertEquals(uuidValue, JavaTypeToFireboltSQLString.transformAny(uuid));

    assertEquals("\\N", JavaTypeToFireboltSQLString.UUID.transform(null));
  }

  @Test
  void shouldTransformShortToString() throws FireboltException {
    short s = 123;
    assertEquals("123", JavaTypeToFireboltSQLString.SHORT.transform(s));

    assertEquals("123", JavaTypeToFireboltSQLString.transformAny(s));

    assertEquals("\\N", JavaTypeToFireboltSQLString.BOOLEAN.transform(null));
  }

  @Test
  void shouldEscapeCharactersWhenTransformingFromString() throws FireboltException {
    assertEquals(
        "'105\\' OR 1=1--\\' '", JavaTypeToFireboltSQLString.STRING.transform("105' OR 1=1--' "));

    assertEquals(
        "'105\\' OR 1=1--\\' '",
        JavaTypeToFireboltSQLString.transformAny("105' OR 1=1--' "));
  }

  @Test
  void shouldTransformLongToString() throws FireboltException {
    assertEquals("105", JavaTypeToFireboltSQLString.LONG.transform(105L));

    assertEquals("105", JavaTypeToFireboltSQLString.transformAny(105L));

    assertEquals("\\N", JavaTypeToFireboltSQLString.LONG.transform(null));
  }

  @Test
  void shouldTransformIntegerToString() throws FireboltException {
    assertEquals("105", JavaTypeToFireboltSQLString.INTEGER.transform(105));

    assertEquals("105", JavaTypeToFireboltSQLString.transformAny(105));

    assertEquals("\\N", JavaTypeToFireboltSQLString.INTEGER.transform(null));
  }

  @Test
  void shouldTransformBigIntegerToString() throws FireboltException {
    assertEquals("1111111111", JavaTypeToFireboltSQLString.BIG_INTEGER.transform(1111111111));

    assertEquals(
        "1111111111", JavaTypeToFireboltSQLString.transformAny(1111111111));

    assertEquals("\\N", JavaTypeToFireboltSQLString.BIG_INTEGER.transform(null));
  }

  @Test
  void shouldTransformFloatToString() throws FireboltException {
    assertEquals("1.5", JavaTypeToFireboltSQLString.FLOAT.transform(1.50f));

    assertEquals("1.5", JavaTypeToFireboltSQLString.transformAny(1.50f));

    assertEquals("\\N", JavaTypeToFireboltSQLString.FLOAT.transform(null));
  }

  @Test
  void shouldTransformDoubleToString() throws FireboltException {
    assertEquals("105", JavaTypeToFireboltSQLString.DOUBLE.transform(105));

    assertEquals("105", JavaTypeToFireboltSQLString.transformAny(105));

    assertEquals("\\N", JavaTypeToFireboltSQLString.DOUBLE.transform(null));
  }

  @Test
  @DefaultTimeZone("Europe/London")
  void shouldTransformDateToString() throws FireboltException {
    Date d = Date.valueOf(LocalDate.of(2022, 5, 23));
    String expectedDateString = "'2022-05-23'";
    assertEquals(expectedDateString, JavaTypeToFireboltSQLString.DATE.transform(d));
    assertEquals(expectedDateString, JavaTypeToFireboltSQLString.transformAny((d)));
  }

  @Test
  void shouldTransformTimeToString() throws FireboltException {
    assertEquals("105", JavaTypeToFireboltSQLString.DOUBLE.transform(105));

    assertEquals("105", JavaTypeToFireboltSQLString.transformAny(105));

    assertEquals("\\N", JavaTypeToFireboltSQLString.DOUBLE.transform(null));
  }

  @Test
  void shouldTransformTimeStampToString() throws FireboltException {
    Timestamp ts = Timestamp.valueOf(LocalDateTime.of(2022, 5, 23, 12, 57, 13, 173456789));
    assertEquals("'2022-05-23 12:57:13.173456789'", JavaTypeToFireboltSQLString.TIMESTAMP.transform(ts));
    assertEquals("'2022-05-23 12:57:13.173456789'", JavaTypeToFireboltSQLString.transformAny(ts));
    assertEquals("\\N", JavaTypeToFireboltSQLString.TIMESTAMP.transform(null));
  }

  @Test
  void shouldTransformSqlArray() throws FireboltException {
    String value = "[1,2,3,\\N,5]";
    FireboltColumn column = FireboltColumn.of("Array(INT32)");
    FireboltArray fireboltArray = SqlArrayUtil.transformToSqlArray(value, column);
    assertEquals(value, JavaTypeToFireboltSQLString.ARRAY.transform(fireboltArray));
  }

  @Test
  void shouldTransformArrayOfArray() throws FireboltException {
    String value = "[['a','b'],['c']]";
    FireboltColumn column = FireboltColumn.of("Array(Array(string))");
    FireboltArray fireboltArray = SqlArrayUtil.transformToSqlArray(value, column);
    assertEquals(value, JavaTypeToFireboltSQLString.ARRAY.transform(fireboltArray));
  }

  @Test
  void shouldTransformJavaArrayOfArray() throws FireboltException {
    String[][] arr = new String[][] {{"a", "b"}, {"c"}};
    assertEquals("[['a','b'],['c']]", JavaTypeToFireboltSQLString.ARRAY.transform(arr));
  }

  @Test
  void shouldTransformJavaArrayOfPrimitives() throws FireboltException {
    int[] arr = {5};
    assertEquals("[5]", JavaTypeToFireboltSQLString.ARRAY.transform(arr));
  }

  @Test
  void shouldTransformEmptyArray() throws FireboltException {
    int[] arr = {};
    assertEquals("[]", JavaTypeToFireboltSQLString.ARRAY.transform(arr));
  }

  @Test
  void shouldThrowExceptionWhenObjectTypeIsNotSupported() {
    Map map = new HashMap<>();
    FireboltException ex =
        assertThrows(
            FireboltException.class,
            () -> JavaTypeToFireboltSQLString.transformAny(map));
    assertEquals(TYPE_NOT_SUPPORTED, ex.getType());
  }

  @Test
  void shouldThrowExceptionWhenObjectCouldNotBeTransformed() {
    Map map = new HashMap<>();
    FireboltException ex =
            assertThrows(
                    FireboltException.class,
                    () -> JavaTypeToFireboltSQLString.ARRAY.transform(map));
    assertEquals(TYPE_TRANSFORMATION_ERROR, ex.getType());
  }
}
