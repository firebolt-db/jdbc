package com.firebolt.jdbc.resultset;

import com.firebolt.jdbc.LoggerUtil;
import com.firebolt.jdbc.statement.FireboltStatement;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junitpioneer.jupiter.DefaultTimeZone;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.InputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalTime;

import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FireboltResultSetTest {

  private InputStream inputStream;
  private ResultSet resultSet;

  @Mock
  private FireboltStatement fireboltStatement;

  @AfterEach
  void closeStream() {
    try {
      inputStream.close();
    } catch (Exception e) {
    }
  }

  @Test
  void shouldReturnMetadata() throws SQLException {
    // This only tests that Metadata is available with the resultSet.
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535, false, fireboltStatement, false);
    assertNotNull(resultSet.getMetaData());
    assertEquals("array_test_table", resultSet.getMetaData().getTableName(1));
    assertEquals("array_test_db", resultSet.getMetaData().getCatalogName(1));
  }

  @Test
  void shouldNotBeLastWhenThereIsMoreData() throws SQLException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    assertFalse(resultSet.isLast());
  }

  @Test
  void shouldNotBeLastAtLastLine() throws SQLException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    resultSet.next();
    resultSet.next();
    assertTrue(resultSet.isLast());
  }

  @Test
  void shouldReadAllTheData() throws SQLException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    resultSet.next();
    assertEquals(1L, resultSet.getObject(1));
    String[][][] firstArray = {{{"1", "2"}, {"3", "4"}}};
    assertArrayEquals(firstArray, ((String[][][]) resultSet.getObject(2)));

    resultSet.next();
    assertEquals(2L, resultSet.getObject(1));
    String[][][] secondArray = {{{"1", "2"}, {"3", "4"}}, {{"5", "6"}, {"7", "8", null}}};
    assertArrayEquals(secondArray, ((String[][][]) resultSet.getObject(2)));
  }

  @Test
  void shouldBeBeforeFirstIfFirstRowNotRead() throws SQLException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    assertTrue(resultSet.isBeforeFirst());
    resultSet.next();
    assertFalse(resultSet.isBeforeFirst());
  }

  @Test
  void shouldGetBigDecimal() throws SQLException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    resultSet.next();
    assertEquals(new BigDecimal("1"), resultSet.getBigDecimal(1));
    assertEquals(new BigDecimal("1"), resultSet.getBigDecimal("id"));
  }

  @Test
  void shouldGetBigDecimalWithScale() throws SQLException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    resultSet.next();
    assertEquals(
        new BigDecimal("1").setScale(2, RoundingMode.HALF_UP), resultSet.getBigDecimal(1, 2));
    assertEquals(
        new BigDecimal("1").setScale(2, RoundingMode.HALF_UP), resultSet.getBigDecimal("id", 2));
  }

  @Test
  void shouldBeFirstWhenNextRecordIsTheFirstToRead() throws SQLException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    resultSet.next();
    assertTrue(resultSet.isFirst());
  }

  @Test
  void shouldBeAfterReadingTheLast() throws SQLException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    while (resultSet.next()) {
      // just read everything
    }
    assertTrue(resultSet.isAfterLast());
    assertFalse(resultSet.isLast());
  }

  @Test
  void shouldReturnFalseWhenCallingWasNullAfterRead() throws SQLException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    resultSet.next();
    resultSet.getInt(1);
    assertFalse(resultSet.wasNull());
  }

  @Test
  void shouldThrowExceptionWhenCallingWasNullBeforeAnyGet() throws SQLException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    resultSet.next();
    assertThrows(IllegalArgumentException.class, () -> resultSet.wasNull(), "A column must be read before checking nullability");
  }

  @Test
  void shouldReturnTrueWhenLastValueGotWasNull() throws SQLException {
    // This only tests that Metadata is available with the resultSet.
    inputStream = getInputStreamWithNulls();
    resultSet = new FireboltResultSet(inputStream);
    resultSet.next();
    resultSet.getObject(2);
    assertTrue(resultSet.wasNull());
    resultSet.next();
    assertTrue(resultSet.wasNull());
  }

  @Test
  void shouldReturnInt() throws SQLException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    resultSet.next();
    assertEquals(1, resultSet.getInt(1));
    assertEquals(1, resultSet.getInt("id"));
    resultSet.next();
    assertEquals(2, resultSet.getInt(1));
    assertEquals(2, resultSet.getInt("id"));
  }

  @Test
  void shouldReturnFloat() throws SQLException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "a_table", "a_db", 65535);
    resultSet.next();
    assertEquals(14.6f, resultSet.getFloat(7));
    assertEquals(14.6f, resultSet.getFloat("a_double"));
    resultSet.next();
    assertEquals(0, resultSet.getFloat(7));
    assertEquals(0, resultSet.getFloat("a_double"));
  }

  @Test
  void shouldReturnDouble() throws SQLException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "a_table", "a_db", 65535);
    resultSet.next();
    assertEquals(14.6d, resultSet.getDouble(7));
    assertEquals(14.6d, resultSet.getDouble("a_double"));
    resultSet.next();
    assertEquals(0, resultSet.getDouble(7));
    assertEquals(0, resultSet.getDouble("a_double"));
  }

  @Test
  void shouldReturnString() throws SQLException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    resultSet.next();
    assertEquals("Taylor's Prime Steak House", resultSet.getString(3));
    assertEquals("Taylor's Prime Steak House", resultSet.getString("name"));
  }

  @Test
  void shouldReturnTypeForward() throws SQLException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    assertEquals(TYPE_FORWARD_ONLY, resultSet.getType());
  }

  @Test
  void shouldReturnBytes() throws SQLException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    resultSet.next();
    assertArrayEquals("Taylor\\'s Prime Steak House".getBytes(), resultSet.getBytes(3));
    assertArrayEquals("Taylor\\'s Prime Steak House".getBytes(), resultSet.getBytes("name"));
    resultSet.next();
    assertNull(resultSet.getBytes(3));
  }

  @Test
  void shouldReturnNullWhenValueIsNull() throws SQLException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    resultSet.next();
    resultSet.next();
    assertNull(resultSet.getBytes(3));
  }

  @Test
  void shouldReturnByte() throws SQLException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    resultSet.next();
    assertEquals(1, resultSet.getByte(1));
    assertEquals(1, resultSet.getByte("id"));
  }

  @Test
  void shouldReturn0ByteWhenValueIsNull() throws SQLException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    resultSet.next();
    resultSet.next();
    assertEquals((byte) 0, resultSet.getByte(3));
  }

  @Test
  void shouldReturnNullWhenValueStringIsNull() throws SQLException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    resultSet.next();
    resultSet.next(); // second line contains \N which represents a null value
    assertNull(resultSet.getString(3));
    assertNull(resultSet.getString("name"));
  }

  @Test
  @DefaultTimeZone("Europe/Paris")
  void shouldReturnDate() throws SQLException, ParseException {
    Date expectedDate = Date.valueOf(LocalDate.of(2022, 5, 10));
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    resultSet.next();

    assertEquals(expectedDate, resultSet.getDate(5));
    assertEquals(expectedDate, resultSet.getDate("a_date"));
  }

  @Test
  @DefaultTimeZone("Europe/Paris")
  void shouldReturnTimeStamp() throws SQLException, ParseException {
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
    java.util.Date parsedDate = dateFormat.parse("2022-05-10 13:01:02");
    Timestamp timestamp = new Timestamp(parsedDate.getTime());

    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    resultSet.next();

    assertEquals(timestamp, resultSet.getTimestamp(4));
    assertEquals(timestamp, resultSet.getTimestamp("a_timestamp"));
  }

  @Test
  void shouldReturnBoolean() throws SQLException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    resultSet.next();

    assertTrue(resultSet.getBoolean(6));
    assertTrue(resultSet.getBoolean("is_online"));
    resultSet.next();
    assertFalse(resultSet.getBoolean(6));
    assertFalse(resultSet.getBoolean("is_online"));
  }

  @Test
  void shouldReturnTime() throws SQLException {
    Time expectedTime = Time.valueOf(LocalTime.of(13, 1, 2));
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    resultSet.next();
    assertEquals(expectedTime, resultSet.getTime(4));
    assertEquals(expectedTime, resultSet.getTime("a_timestamp"));
  }

  @Test
  void shouldGetArray() throws SQLException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    resultSet.next();
    String[][][] firstArray = {{{"1", "2"}, {"3", "4"}}};
    assertArrayEquals(firstArray, ((String[][][]) (resultSet.getArray("arr")).getArray()));
    assertArrayEquals(firstArray, ((String[][][]) (resultSet.getArray(2)).getArray()));
  }

  @Test
  void shouldReturnUnescapedString() throws SQLException {
    String expected =
        "[0] [Aggregate] GroupBy: [] Aggregates: [COUNT(DISTINCT FB_NODE_2.a1), APPROX_COUNT_DISTINCT(FB_NODE_2.a1)] @ FB_NODE_1\n \\_[1] [StoredTable] Name: 'ft', used 1/1 column(s) FACT @ FB_NODE_2\n";
    inputStream = getInputStreamWitExplain();
    resultSet = new FireboltResultSet(inputStream, "any", "any", 65535);
    resultSet.next();
    assertEquals(expected, resultSet.getString(1));
    assertEquals(expected, resultSet.getObject(1));
  }

  @Test
  void shouldThrowException() throws SQLException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "empty_test", "empty_test", 65535);
    resultSet.next();
    resultSet.next(); // second line contains \N which represents a null value
    assertNull(resultSet.getString(3));
    assertNull(resultSet.getString("name"));
  }

  @Test
  void shouldThrowExceptionWhenCheckingWasNullAfterClose() throws SQLException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    resultSet.close();
    assertThrows(SQLException.class, resultSet::wasNull);
  }

  @Test
  void shouldThrowExceptionWhenGettingValueAfterClose() throws SQLException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    resultSet.close();
    assertThrows(SQLException.class, () -> resultSet.getObject(1));
  }

  @Test
  void shouldThrowSQLExceptionWhenGettingValueWithInvalidColumnIndex() throws SQLException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    resultSet.close();
    assertThrows(SQLException.class, () -> resultSet.getObject("INVALID_COLUMN"));
  }

  @Test
  void shouldCloseStream() throws SQLException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    assertFalse(resultSet.isClosed());
    resultSet.close();
    assertTrue(resultSet.isClosed());
  }

  @Test
  void shouldCloseStatementWhenCloseOnCompletion() throws SQLException {
    when(fireboltStatement.isCloseOnCompletion()).thenReturn(true);
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535, fireboltStatement);
    resultSet.close();
    verify(fireboltStatement).close();
  }

  @Test
  void shouldNotCloseStatementWhenNotCloseOnCompletion() throws SQLException {
    when(fireboltStatement.isCloseOnCompletion()).thenReturn(false);
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535, fireboltStatement);
    resultSet.close();
    verifyNoMoreInteractions(fireboltStatement);
  }

  @Test
  void shouldNotThrowExceptionWhenClosingTwice() throws SQLException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    resultSet.close();
    assertTrue(resultSet.isClosed());
    try {
      resultSet.close();
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  void shouldThrowExceptionWhenColumnDoesNotExist() throws SQLException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    assertThrows(SQLException.class, () -> resultSet.getObject(50));
  }

  @Test
  void shouldReturnEmptyWhenValueFoundIsEmpty() throws SQLException {
    inputStream = getInputStreamWithEmpty();
    resultSet = new FireboltResultSet(inputStream, "table_with_empty", "db_with_emtpy", 65535);
    resultSet.next();
    assertEquals(StringUtils.EMPTY, resultSet.getObject("name"));
    assertEquals(StringUtils.EMPTY, resultSet.getObject("city"));
  }

  @Test
  void shouldLogResultSet() throws SQLException {
    // This only tests that Metadata is available with the resultSet.
    inputStream = getInputStreamWithArray();
    try (MockedStatic<LoggerUtil> loggerUtilMockedStatic = mockStatic(LoggerUtil.class)) {
      loggerUtilMockedStatic.when(() -> LoggerUtil.logInputStream(inputStream)).thenReturn(inputStream);
      resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535, false, fireboltStatement, true);
      loggerUtilMockedStatic.verify(() -> LoggerUtil.logInputStream(inputStream));
    }
  }

  private InputStream getInputStreamWithArray() {
    return FireboltResultSetTest.class.getResourceAsStream("/responses/firebolt-response-example");
  }

  private InputStream getInputStreamWithEmpty() {
    return FireboltResultSetTest.class.getResourceAsStream(
        "/responses/firebolt-response-with-empty");
  }

  private InputStream getInputStreamWithNulls() {
    return FireboltResultSetTest.class.getResourceAsStream(
            "/responses/firebolt-response-with-nulls");
  }

  private InputStream getInputStreamWitExplain() {
    return FireboltResultSetTest.class.getResourceAsStream(
        "/responses/firebolt-response-with-escape-characters");
  }
}
