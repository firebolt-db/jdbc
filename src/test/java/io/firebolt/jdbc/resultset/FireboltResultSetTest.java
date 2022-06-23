package io.firebolt.jdbc.resultset;

import io.firebolt.jdbc.resultset.type.array.FireboltArray;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static org.junit.jupiter.api.Assertions.*;

class FireboltResultSetTest {

  InputStream inputStream;
  ResultSet resultSet;

  @BeforeAll
  public static void beforeAll() {
    TimeZone.setDefault(TimeZone.getTimeZone("Europe/London"));
  }

  @AfterEach
  void closeStream() {
    try {
      inputStream.close();
    } catch (Exception e) {
    }
  }

  @Test
  void shouldReturnMetadata() throws SQLException, IOException {
    // This only tests that Metadata is available with the resultSet.
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    assertNotNull(resultSet.getMetaData());
    assertEquals("array_test_table", resultSet.getMetaData().getTableName(1));
    assertEquals("array_test_db", resultSet.getMetaData().getCatalogName(1));
  }

  @Test
  void shouldNotBeLastWhenThereIsMoreData() throws SQLException, IOException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    assertFalse(resultSet.isLast());
  }

  @Test
  void shouldNotBeLastAtLastLine() throws SQLException, IOException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    resultSet.next();
    resultSet.next();
    assertTrue(resultSet.isLast());
  }

  @Test
  void shouldReadAllTheData() throws SQLException, IOException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    resultSet.next();
    assertEquals(1L, resultSet.getObject(1));
    String[][][] firstArray = {{{"1", "2"}, {"3", "4"}}};
    assertArrayEquals(
        firstArray, ((String[][][]) ((FireboltArray) resultSet.getObject(2)).getArray()));

    resultSet.next();
    assertEquals(2L, resultSet.getObject(1));
    String[][][] secondArray = {{{"1", "2"}, {"3", "4"}}, {{"5", "6"}, {"7", "8"}}};
    assertArrayEquals(
        secondArray, ((String[][][]) ((FireboltArray) resultSet.getObject(2)).getArray()));
  }

  @Test
  void shouldBeBeforeFirstIfFirstRowNotRead() throws SQLException, IOException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    assertTrue(resultSet.isBeforeFirst());
    resultSet.next();
    assertFalse(resultSet.isBeforeFirst());
  }

  @Test
  void shouldGetBigDecimal() throws SQLException, IOException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    resultSet.next();
    assertEquals(new BigDecimal("1"), resultSet.getBigDecimal(1));
    assertEquals(new BigDecimal("1"), resultSet.getBigDecimal("id"));
  }

  @Test
  void shouldGetBigDecimalWithScale() throws SQLException, IOException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    resultSet.next();
    assertEquals(
        new BigDecimal("1").setScale(2, RoundingMode.HALF_UP), resultSet.getBigDecimal(1, 2));
    assertEquals(
        new BigDecimal("1").setScale(2, RoundingMode.HALF_UP), resultSet.getBigDecimal("id", 2));
  }

  @Test
  void shouldBeFirstWhenNextRecordIsTheFirstToRead() throws SQLException, IOException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    resultSet.next();
    assertTrue(resultSet.isFirst());
  }

  @Test
  void shouldBeAfterReadingTheLast() throws SQLException, IOException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    while (resultSet.next()) {
      // just read everything
    }
    assertTrue(resultSet.isAfterLast());
    assertFalse(resultSet.isLast());
  }

  @Test
  void shouldReturnFalseWhenCallingWasNullAfterRead() throws SQLException, IOException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    resultSet.next();
    resultSet.getInt(1);
    assertFalse(resultSet.wasNull());
  }

  @Test
  void shouldReturnFalseWhenCallingWasNullBeforeAnyRead() throws SQLException, IOException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    resultSet.next();
    assertFalse(resultSet.wasNull());
  }

  @Test
  void shouldReturnInt() throws SQLException, IOException {
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
  void shouldReturnString() throws SQLException, IOException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    resultSet.next();
    assertEquals("Taylor\\'s Prime Steak House", resultSet.getString(3));
    assertEquals("Taylor\\'s Prime Steak House", resultSet.getString("name"));
  }

  @Test
  void shouldReturnTypeForward() throws SQLException, IOException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    assertEquals(TYPE_FORWARD_ONLY, resultSet.getType());
  }

  @Test
  void shouldReturnBytes() throws SQLException, IOException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    resultSet.next();
    assertArrayEquals("Taylor\\'s Prime Steak House".getBytes(), resultSet.getBytes(3));
    assertArrayEquals("Taylor\\'s Prime Steak House".getBytes(), resultSet.getBytes("name"));
    resultSet.next();
    assertNull(resultSet.getBytes(3));
  }

  @Test
  void shouldReturnNullWhenValueIsNull() throws SQLException, IOException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    resultSet.next();
    resultSet.next();
    assertNull(resultSet.getBytes(3));
  }

  @Test
  void shouldReturnByte() throws SQLException, IOException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    resultSet.next();
    assertEquals(1, resultSet.getByte(1));
    assertEquals(1, resultSet.getByte("id"));
  }

  @Test
  void shouldReturn0ByteWhenValueIsNull() throws SQLException, IOException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    resultSet.next();
    resultSet.next();
    assertEquals((byte) 0, resultSet.getByte(3));
  }

  @Test
  void shouldReturnNullWhenValueStringIsNull() throws SQLException, IOException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    resultSet.next();
    resultSet.next(); // second line contains \N which represents a null value
    assertNull(resultSet.getString(3));
    assertNull(resultSet.getString("name"));
  }

  @Test
  void shouldReturnDate() throws SQLException, ParseException {
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    java.util.Date parsedDate = dateFormat.parse("2022-05-10");
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    resultSet.next();

    assertEquals(parsedDate, resultSet.getDate(5));
    assertEquals(parsedDate, resultSet.getDate("a_date"));
  }

  @Test
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

    assertTrue(resultSet.getBoolean(1));
    assertTrue(resultSet.getBoolean("is_online"));
    resultSet.next();
    assertFalse(resultSet.getBoolean(1));
    assertFalse(resultSet.getBoolean("is_online"));
  }

  @Test
  void shouldReturnTime() throws SQLException, IOException, ParseException {
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
    java.util.Date parsedDate = dateFormat.parse("2022-05-10 13:01:02");
    Timestamp timestamp = new Timestamp(parsedDate.getTime());
    Time time = new Time(timestamp.getTime());

    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    resultSet.next();

    assertEquals(time, resultSet.getTime(4));
    assertEquals(time, resultSet.getTime("a_timestamp"));
  }

  @Test
  void shouldGetArray() throws SQLException, IOException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    resultSet.next();
    String[][][] firstArray = {{{"1", "2"}, {"3", "4"}}};
    assertArrayEquals(firstArray, ((String[][][]) (resultSet.getArray("arr")).getArray()));
    assertArrayEquals(firstArray, ((String[][][]) (resultSet.getArray(2)).getArray()));
  }

  @Test
  void shouldThrowException() throws SQLException, IOException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "empty_test", "empty_test", 65535);
    resultSet.next();
    resultSet.next(); // second line contains \N which represents a null value
    assertNull(resultSet.getString(3));
    assertNull(resultSet.getString("name"));
  }

  @Test
  void shouldThrowExceptionWhenCheckingWasNullAfterClose() throws SQLException, IOException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    resultSet.close();
    assertThrows(SQLException.class, resultSet::wasNull);
  }

  @Test
  void shouldThrowExceptionWhenGettingValueAfterClose() throws SQLException, IOException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    resultSet.close();
    assertThrows(SQLException.class, () -> resultSet.getObject(1));
  }

  @Test
  void shouldThrowSQLExceptionWhenGettingValueWithInvalidColumnIndex()
      throws SQLException, IOException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    resultSet.close();
    assertThrows(SQLException.class, () -> resultSet.getObject("INVALID_COLUMN"));
  }

  @Test
  void shouldCloseStream() throws SQLException, IOException {
    inputStream = getInputStreamWithArray();
    resultSet = new FireboltResultSet(inputStream, "array_test_table", "array_test_db", 65535);
    assertFalse(resultSet.isClosed());
    resultSet.close();
    assertTrue(resultSet.isClosed());
  }

  @Test
  void shouldNotThrowExceptionWhenClosingTwice() throws SQLException, IOException {
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
  void shouldThrowExceptionWhenColumnDoesNotExist() throws SQLException, IOException {
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

  private InputStream getInputStreamWithArray() {
    return FireboltResultSetTest.class.getResourceAsStream("/responses/firebolt-response-example");
  }

  private InputStream getInputStreamWithEmpty() {
    return FireboltResultSetTest.class.getResourceAsStream("/responses/firebolt-response-with-empty-example");
  }

}
