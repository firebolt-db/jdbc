package com.firebolt.jdbc.resultset;

import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.statement.FireboltStatement;
import com.firebolt.jdbc.util.LoggerUtil;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junitpioneer.jupiter.DefaultTimeZone;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.util.Calendar;
import java.util.TimeZone;

import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DefaultTimeZone("UTC")
class FireboltResultSetTest {

	private static final TimeZone UTC_TZ = TimeZone.getTimeZone("UTC");
	private final static Calendar EST_CALENDAR = Calendar.getInstance(TimeZone.getTimeZone("EST"));
	private final static Calendar UTC_CALENDAR = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
	private InputStream inputStream;
	private ResultSet resultSet;
	@Mock
	private FireboltStatement fireboltStatement;

	@AfterEach
	void afterEach() throws SQLException, IOException {
		inputStream.close();
		resultSet.close();
	}

	@Test
	void shouldReturnMetadata() throws SQLException {
		// This only tests that Metadata is available with the resultSet.
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535, false, fireboltStatement, false);
		assertNotNull(resultSet.getMetaData());
		assertEquals("any_name", resultSet.getMetaData().getTableName(1));
		assertEquals("array_db", resultSet.getMetaData().getCatalogName(1));
	}

	@Test
	void shouldNotBeLastWhenThereIsMoreData() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535);
		assertFalse(resultSet.isLast());
	}

	@Test
	void shouldNotBeLastAtLastLine() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535);
		resultSet.next();
		resultSet.next();
		assertTrue(resultSet.isLast());
	}

	@Test
	void shouldReadAllTheData() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535);
		resultSet.next();
		assertEquals(1, resultSet.getObject(1));
		String[][][] firstArray = { { { "1", "2" }, { "3", "4" } } };
		Array array = resultSet.getObject(2, Array.class);
		assertArrayEquals(firstArray, (String[][][]) array.getArray());
		assertEquals(1L, resultSet.getObject(1, Long.class));

		resultSet.next();
		assertEquals(2, resultSet.getObject(1));
		String[][][] secondArray = { { { "1", "2" }, { "3", "4" } }, { { "5", "6" }, { "7", "8", null } } };
		assertArrayEquals(secondArray, ((String[][][]) resultSet.getObject(2)));
	}

	@Test
	void shouldBeBeforeFirstIfFirstRowNotRead() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535);
		assertTrue(resultSet.isBeforeFirst());
		resultSet.next();
		assertFalse(resultSet.isBeforeFirst());
	}

	@Test
	void shouldGetBigDecimal() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535);
		resultSet.next();
		assertEquals(new BigDecimal("1"), resultSet.getBigDecimal(1));
		assertEquals(new BigDecimal("1"), resultSet.getBigDecimal("id"));
	}

	@Test
	@SuppressWarnings("deprecation") // ResultSet.getBigDecimal() is deprecated but  still has to be tested.
	void shouldGetBigDecimalWithScale() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535);
		resultSet.next();
		assertEquals(new BigDecimal("1").setScale(2, RoundingMode.HALF_UP), resultSet.getBigDecimal(1, 2));
		assertEquals(new BigDecimal("1").setScale(2, RoundingMode.HALF_UP), resultSet.getBigDecimal("id", 2));
	}

	@Test
	void shouldBeFirstWhenNextRecordIsTheFirstToRead() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535);
		resultSet.next();
		assertTrue(resultSet.isFirst());
		resultSet.next();
		assertFalse(resultSet.isFirst());
	}

	@Test
	void shouldBeAfterReadingTheLast() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535);
		assertFalse(resultSet.isAfterLast());
		assertFalse(resultSet.isLast());
		while (resultSet.next()) {
			// just read everything
			assertFalse(resultSet.isAfterLast());
		}
		assertTrue(resultSet.isAfterLast());
		assertFalse(resultSet.isLast());
	}

	@Test
	void shouldReturnFalseWhenCallingWasNullAfterRead() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535);
		resultSet.next();
		resultSet.getInt(1);
		assertFalse(resultSet.wasNull());
	}

	@Test
	void shouldThrowExceptionWhenCallingWasNullBeforeAnyGet() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535);
		resultSet.next();
		assertThrows(IllegalArgumentException.class, () -> resultSet.wasNull(),
				"A column must be read before checking nullability");
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
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535);
		resultSet.next();
		assertEquals(1, resultSet.getInt(1));
		assertEquals(1, resultSet.getInt("id"));
		assertEquals(1, resultSet.getObject(1, Long.class));

		resultSet.next();
		assertEquals(2, resultSet.getInt(1));
		assertEquals(2, resultSet.getInt("id"));
		assertEquals(2, resultSet.getObject(1, Long.class));

	}

	@Test
	void shouldReturnFloat() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = new FireboltResultSet(inputStream, "a_table", "a_db", 65535);
		resultSet.next();
		assertEquals(14.6f, resultSet.getFloat(6));
		assertEquals(14.6f, resultSet.getFloat("a_double"));
		assertEquals(14.6f, resultSet.getObject(6, Float.class));

		resultSet.next();
		assertEquals(0, resultSet.getFloat(6));
		assertEquals(0, resultSet.getFloat("a_double"));
		assertNull(resultSet.getObject(6, Float.class));

	}

	@Test
	void shouldReturnDouble() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = new FireboltResultSet(inputStream, "a_table", "a_db", 65535);
		resultSet.next();
		assertEquals(14.6d, resultSet.getDouble(6));
		assertEquals(14.6d, resultSet.getDouble("a_double"));
		resultSet.next();
		assertEquals(0, resultSet.getDouble(6));
		assertEquals(0, resultSet.getDouble("a_double"));
	}

	@Test
	void shouldReturnString() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535);
		resultSet.next();
		String expected = "Taylor's Prime Steak House";
		assertEquals(expected, resultSet.getString(3));
		assertEquals(expected, resultSet.getString("name"));
		assertEquals(expected, resultSet.getObject(3, String.class));

	}

	@Test
	void shouldReturnShort() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535);
		resultSet.next();
		assertEquals(5, resultSet.getShort("an_integer"));
		assertEquals(5, resultSet.getShort(7));
		resultSet.next();
		assertEquals(0, resultSet.getShort("an_integer"));
		assertEquals(0, resultSet.getShort(7));
	}

	@Test
	void shouldReturnTypeForward() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535);
		assertEquals(TYPE_FORWARD_ONLY, resultSet.getType());
	}

	@Test
	void shouldReturnBytes() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535);
		resultSet.next();
		byte[] expected = "Taylor\\'s Prime Steak House".getBytes();
		assertArrayEquals(expected, resultSet.getBytes(3));
		assertArrayEquals(expected, resultSet.getBytes("name"));
		assertArrayEquals(expected, resultSet.getObject(3, byte[].class));
		assertArrayEquals(expected, resultSet.getObject("name", byte[].class));
		resultSet.next();
		assertNull(resultSet.getBytes(3));
	}

	@Test
	void shouldReturnNullWhenValueIsNull() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535);
		resultSet.next();
		resultSet.next();
		assertNull(resultSet.getBytes(3));
	}

	@Test
	void shouldReturnByte() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535);
		resultSet.next();
		assertEquals(1, resultSet.getByte(1));
		assertEquals(1, resultSet.getByte("id"));
	}

	@Test
	void shouldReturn0ByteWhenValueIsNull() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535);
		resultSet.next();
		resultSet.next();
		assertEquals((byte) 0, resultSet.getByte(3));
	}

	@Test
	void shouldReturnNullWhenValueStringIsNull() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535);
		resultSet.next();
		resultSet.next(); // second line contains \N which represents a null value
		assertNull(resultSet.getString(3));
		assertNull(resultSet.getString("name"));
	}

	@Test
	void shouldReturnDate() throws SQLException {
		Date expectedDate = Date.valueOf(LocalDate.of(2022, 5, 10));
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535);
		resultSet.next();

		assertEquals(expectedDate, resultSet.getDate(4));
		assertEquals(expectedDate, resultSet.getDate("a_date"));
	}

	@Test
	void shouldReturnTimeStamp() throws SQLException, ParseException {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		java.util.Date parsedDate = dateFormat.parse("2022-05-10 13:01:02");
		Timestamp timestamp = new Timestamp(parsedDate.getTime());

		inputStream = getInputStreamWithDates();
		resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535);
		resultSet.next();

		assertEquals(timestamp, resultSet.getTimestamp(2));
		assertEquals(timestamp, resultSet.getTimestamp("a_datetime"));
	}

	@Test
	void shouldReturnBoolean() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535);
		resultSet.next();

		assertTrue(resultSet.getBoolean(5));
		assertTrue(resultSet.getBoolean("is_online"));
		resultSet.next();
		assertFalse(resultSet.getBoolean(5));
		assertFalse(resultSet.getBoolean("is_online"));
	}

	@Test
	void shouldReturnTime() throws SQLException {
		Time expectedTime = new Time(
				ZonedDateTime.of(1970, 1, 1, 13, 1, 2, 0, UTC_TZ.toZoneId()).toInstant().toEpochMilli());
		inputStream = getInputStreamWithDates();
		resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535);
		resultSet.next();
		assertEquals(expectedTime, resultSet.getTime(2));
		assertEquals(expectedTime, resultSet.getTime("a_datetime"));
	}

	@Test
	void shouldGetArray() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535);
		resultSet.next();
		String[][][] firstArray = { { { "1", "2" }, { "3", "4" } } };
		assertArrayEquals(firstArray, ((String[][][]) (resultSet.getArray("arr")).getArray()));
		assertArrayEquals(firstArray, ((String[][][]) (resultSet.getArray(2)).getArray()));
	}

	@Test
	void shouldReturnUnescapedString() throws SQLException {
		String expected = "[0] [Aggregate] GroupBy: [] Aggregates: [COUNT(DISTINCT FB_NODE_2.a1), APPROX_COUNT_DISTINCT(FB_NODE_2.a1)] @ FB_NODE_1\n \\_[1] [StoredTable] Name: 'ft', used 1/1 column(s) FACT @ FB_NODE_2\n";
		inputStream = getInputStreamWitExplain();
		resultSet = new FireboltResultSet(inputStream, "any", "any", 65535);
		resultSet.next();
		assertEquals(expected, resultSet.getString(1));
		assertEquals(expected, resultSet.getObject(1));
	}

	@Test
	void shouldThrowException() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = new FireboltResultSet(inputStream, "empty_test", "empty_test", 65535);
		resultSet.next();
		resultSet.next(); // second line contains \N which represents a null value
		assertNull(resultSet.getString(3));
		assertNull(resultSet.getString("name"));
	}

	@Test
	void shouldThrowExceptionWhenCheckingWasNullAfterClose() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535);
		resultSet.close();
		assertThrows(SQLException.class, resultSet::wasNull);
	}

	@Test
	void shouldThrowExceptionWhenGettingValueAfterClose() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535);
		resultSet.close();
		assertThrows(SQLException.class, () -> resultSet.getObject(1));
	}

	@Test
	void shouldThrowSQLExceptionWhenGettingValueWithInvalidColumnIndex() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535);
		resultSet.close();
		assertThrows(SQLException.class, () -> resultSet.getObject("INVALID_COLUMN"));
	}

	@Test
	void shouldCloseStream() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535);
		assertFalse(resultSet.isClosed());
		resultSet.close();
		assertTrue(resultSet.isClosed());
	}

	@Test
	void shouldCloseStatementWhenCloseOnCompletion() throws SQLException {
		when(fireboltStatement.isCloseOnCompletion()).thenReturn(true);
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535, fireboltStatement);
		resultSet.close();
		verify(fireboltStatement).close();
	}

	@Test
	void shouldNotCloseStatementWhenNotCloseOnCompletion() throws SQLException {
		when(fireboltStatement.isCloseOnCompletion()).thenReturn(false);
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535, fireboltStatement);
		resultSet.close();
		verifyNoMoreInteractions(fireboltStatement);
	}

	@Test
	void shouldNotThrowExceptionWhenClosingTwice() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535);
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
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535);
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
		inputStream = getInputStreamWithCommonResponseExample();
		try (MockedStatic<LoggerUtil> loggerUtilMockedStatic = mockStatic(LoggerUtil.class)) {
			loggerUtilMockedStatic.when(() -> LoggerUtil.logInputStream(inputStream)).thenReturn(inputStream);
			resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535, false, fireboltStatement,
					true);
			loggerUtilMockedStatic.verify(() -> LoggerUtil.logInputStream(inputStream));
		}
	}

	@Test
	void shouldGetTimeWithTimezoneFromCalendar() throws SQLException {
		inputStream = getInputStreamWithDates();
		try (MockedStatic<LoggerUtil> loggerUtilMockedStatic = mockStatic(LoggerUtil.class)) {
			loggerUtilMockedStatic.when(() -> LoggerUtil.logInputStream(inputStream)).thenReturn(inputStream);
			resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535, false, fireboltStatement,
					true);
			resultSet.next();

			Time firstExpectedTime = new Time(
					ZonedDateTime.of(1970, 1, 1, 18, 1, 2, 0, UTC_TZ.toZoneId()).toInstant().toEpochMilli());

			Time secondExpectedTime = new Time(
					ZonedDateTime.of(1970, 1, 1, 13, 1, 2, 0, UTC_TZ.toZoneId()).toInstant().toEpochMilli());

			assertEquals(firstExpectedTime, resultSet.getTime("a_datetime", EST_CALENDAR));
			assertEquals(secondExpectedTime, resultSet.getTime("a_datetime", UTC_CALENDAR));
			assertEquals(secondExpectedTime, resultSet.getTime("a_datetime", null));

		}
	}

	@Test
	void shouldGetTimestampWithTimezoneFromCalendar() throws SQLException {
		inputStream = getInputStreamWithDates();
		try (MockedStatic<LoggerUtil> loggerUtilMockedStatic = mockStatic(LoggerUtil.class)) {
			loggerUtilMockedStatic.when(() -> LoggerUtil.logInputStream(inputStream)).thenReturn(inputStream);
			resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535, false, fireboltStatement,
					true);
			resultSet.next();
			Timestamp firstTimeStampFromEST = Timestamp
					.valueOf(ZonedDateTime.of(2022, 5, 10, 18, 1, 2, 0, UTC_TZ.toZoneId()).toLocalDateTime());
			Timestamp firstTimeStampFromUTC = Timestamp
					.valueOf(ZonedDateTime.of(2022, 5, 10, 13, 1, 2, 0, UTC_TZ.toZoneId()).toLocalDateTime());
			Timestamp secondTimeStampFromUTC = Timestamp
					.valueOf(ZonedDateTime.of(2022, 5, 11, 4, 1, 2, 0, UTC_TZ.toZoneId()).toLocalDateTime());

			assertEquals(firstTimeStampFromEST, resultSet.getTimestamp("a_datetime", EST_CALENDAR));
			assertEquals(firstTimeStampFromUTC, resultSet.getTimestamp("a_datetime", UTC_CALENDAR));
			assertEquals(firstTimeStampFromUTC, resultSet.getTimestamp("a_datetime", null));
			resultSet.next();
			assertEquals(secondTimeStampFromUTC, resultSet.getTimestamp("a_datetime", EST_CALENDAR));
		}
	}

	@Test
	void shouldGetTimeObjectsWithTimeZoneFromResponse() throws SQLException {
		inputStream = getInputStreamWithDates();
		try (MockedStatic<LoggerUtil> loggerUtilMockedStatic = mockStatic(LoggerUtil.class)) {
			loggerUtilMockedStatic.when(() -> LoggerUtil.logInputStream(inputStream)).thenReturn(inputStream);
			resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535, false, fireboltStatement,
					false);
			resultSet.next();
			ZonedDateTime zonedDateTime = ZonedDateTime.of(2022, 5, 10, 18, 1, 2, 0, UTC_TZ.toZoneId());

			Timestamp expectedTimestamp = new Timestamp(zonedDateTime.toInstant().toEpochMilli());

			Time expectedTime = new Time(
					zonedDateTime.withYear(1970).withMonth(1).withDayOfMonth(1).toInstant().toEpochMilli());
			Date expectedDate = new Date(ZonedDateTime
					.of(2022, 5, 10, 5, 0, 0, 0, TimeZone.getTimeZone("UTC").toZoneId()).toInstant().toEpochMilli());

			// The timezone returned by the db is always used regardless of the timezone
			// passed as an argument
			assertEquals(expectedTime, resultSet.getTime("a_datetime64_with_tz", EST_CALENDAR));
			assertEquals(expectedTime, resultSet.getTime("a_datetime64_with_tz", UTC_CALENDAR));
			assertEquals(expectedTime, resultSet.getTime("a_datetime64_with_tz", null));

			assertEquals(expectedTimestamp, resultSet.getTimestamp("a_datetime64_with_tz", EST_CALENDAR));
			assertEquals(expectedTimestamp, resultSet.getTimestamp("a_datetime64_with_tz", UTC_CALENDAR));
			assertEquals(expectedTimestamp, resultSet.getTimestamp("a_datetime64_with_tz", null));
			resultSet.next();
			assertEquals(expectedDate, resultSet.getDate("a_datetime64_with_tz", UTC_CALENDAR));
		}
	}

	@Test
	void shouldGetDateWithTimezoneFromCalendar() throws SQLException {
		inputStream = getInputStreamWithDates();
		try (MockedStatic<LoggerUtil> loggerUtilMockedStatic = mockStatic(LoggerUtil.class)) {
			loggerUtilMockedStatic.when(() -> LoggerUtil.logInputStream(inputStream)).thenReturn(inputStream);
			resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535, false, fireboltStatement,
					true);
			resultSet.next();
			Date firstExpectedDateFromEST = new Date(ZonedDateTime
					.of(2022, 5, 10, 5, 0, 0, 0, TimeZone.getTimeZone("UTC").toZoneId()).toInstant().toEpochMilli());
			Date secondExpectedDateFromEST = new Date(ZonedDateTime
					.of(2022, 5, 10, 5, 0, 0, 0, TimeZone.getTimeZone("UTC").toZoneId()).toInstant().toEpochMilli());
			Date secondExpectedDateFromUTC = new Date(ZonedDateTime
					.of(2022, 5, 10, 0, 0, 0, 0, TimeZone.getTimeZone("UTC").toZoneId()).toInstant().toEpochMilli());

			assertEquals(firstExpectedDateFromEST, resultSet.getDate("a_datetime", EST_CALENDAR));
			resultSet.next();
			assertEquals(secondExpectedDateFromEST, resultSet.getDate("a_datetime", EST_CALENDAR));
			assertEquals(secondExpectedDateFromUTC, resultSet.getDate("a_datetime", UTC_CALENDAR));
		}
	}

	@Test
	void shouldFindNullByteA() throws SQLException {
		inputStream = getInputStreamWithByteA();
		resultSet = new FireboltResultSet(inputStream, "any", "any", 65535);
		resultSet.next();
		assertNull(resultSet.getObject("null_bytea"));
	}

	@Test
	void shouldFindByteAWithValue() throws SQLException {
		inputStream = getInputStreamWithByteA();
		resultSet = new FireboltResultSet(inputStream, "any", "any", 65535);
		resultSet.next();
		assertArrayEquals(new byte[] { -34, -83, -66, -17 }, (byte[]) resultSet.getObject("a_bytea"));
		assertEquals("\\xdeadbeef", resultSet.getString("a_bytea"));
		resultSet.next();
		assertArrayEquals(new byte[] { 0, -85 }, resultSet.getObject("a_bytea", byte[].class));
		assertEquals("\\x00ab", resultSet.getString("a_bytea"));
	}

	@Test
	void shouldFindEmptyByteA() throws SQLException {
		inputStream = getInputStreamWithByteA();
		resultSet = new FireboltResultSet(inputStream, "any", "any", 65535);
		resultSet.next();
		assertArrayEquals(new byte[] {}, (byte[]) resultSet.getObject("an_empty_bytea"));
		assertEquals("", resultSet.getString("an_empty_bytea"));
	}

	@Test
	void shouldReturnTrueWhenBooleanFoundIsTrue() throws SQLException {
		inputStream = getInputStreamWithBooleans();
		resultSet = new FireboltResultSet(inputStream, "any", "any", 65535);
		resultSet.next();
		assertTrue((Boolean) resultSet.getObject("true_boolean"));
		assertTrue(resultSet.getBoolean("true_boolean"));
		assertTrue(resultSet.getObject("true_boolean", Boolean.class));
		resultSet.next();
		assertTrue((Boolean) resultSet.getObject("true_boolean"));
		assertTrue(resultSet.getBoolean("true_boolean"));
	}

	@Test
	void shouldReturnFalseWhenBooleanFoundIsFalse() throws SQLException {
		inputStream = getInputStreamWithBooleans();
		resultSet = new FireboltResultSet(inputStream, "any", "any", 65535);
		resultSet.next();
		assertFalse((Boolean) resultSet.getObject("false_boolean"));
		assertFalse(resultSet.getBoolean("false_boolean"));
		assertFalse(resultSet.getObject("false_boolean", Boolean.class));
		resultSet.next();
		assertFalse((Boolean) resultSet.getObject("false_boolean"));
		assertFalse(resultSet.getBoolean("false_boolean"));
	}

	@Test
	void shouldReturnFalseWhenBooleanFoundIsNull() throws SQLException {
		inputStream = getInputStreamWithBooleans();
		resultSet = new FireboltResultSet(inputStream, "any", "any", 65535);
		resultSet.next();
		assertNull(resultSet.getObject("null_boolean"));
		assertFalse(resultSet.getBoolean("null_boolean"));
	}

	@Test
	void shouldThrowExceptionWhenBooleanValueIsInvalid() throws SQLException {
		inputStream = getInputStreamWithBooleans();
		resultSet = new FireboltResultSet(inputStream, "any", "any", 65535);
		resultSet.next();
		assertThrows(FireboltException.class, () -> resultSet.getObject("invalid_boolean"));
	}

	@Test
	void shouldReturnTimestampFromTimestampntz() throws SQLException {
		Timestamp expectedTimestamp = new Timestamp(
				ZonedDateTime.of(2022, 5, 10, 23, 1, 2, 0, UTC_TZ.toZoneId()).toInstant().toEpochMilli());

		inputStream = getInputStreamWithDates();
		resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535);
		resultSet.next();

		assertEquals(expectedTimestamp, resultSet.getTimestamp("timestampntz"));
		assertEquals(expectedTimestamp, resultSet.getObject("timestampntz"));
		Timestamp expectedTimestampWithDifferentTz = new Timestamp(
				ZonedDateTime.of(2022, 5, 11, 4, 1, 2, 0, UTC_TZ.toZoneId()).toInstant().toEpochMilli());
		assertEquals(expectedTimestampWithDifferentTz, resultSet.getTimestamp("timestampntz", EST_CALENDAR));
	}

	@Test
	void shouldReturnDateFromTimestampntz() throws SQLException {
		Date expectedDate = new Date(
				ZonedDateTime.of(2022, 5, 10, 0, 0, 0, 0, UTC_TZ.toZoneId()).toInstant().toEpochMilli());

		inputStream = getInputStreamWithDates();
		resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535);
		resultSet.next();

		assertEquals(expectedDate, resultSet.getDate("timestampntz"));

		Date expectedDateEST = new Date(
				ZonedDateTime.of(2022, 5, 10, 5, 0, 0, 0, UTC_TZ.toZoneId()).toInstant().toEpochMilli());
		assertEquals(expectedDateEST, resultSet.getDate("timestampntz", EST_CALENDAR));
	}

	@Test
	void shouldReturnTimeFromTimestampntz() throws SQLException {
		Time expectedTime = new Time(
				ZonedDateTime.of(1970, 1, 1, 23, 1, 2, 0, UTC_TZ.toZoneId()).toInstant().toEpochMilli());
		Time expectedTimeFromEST = new Time(
				ZonedDateTime.of(1970, 1, 2, 4, 1, 2, 0, UTC_TZ.toZoneId()).toInstant().toEpochMilli());
		inputStream = getInputStreamWithDates();
		resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535);
		resultSet.next();
		assertEquals(expectedTime, resultSet.getTime("timestampntz"));
		assertEquals(expectedTimeFromEST, resultSet.getTime("timestampntz", EST_CALENDAR));
	}

	@Test
	void shouldReturnTimeFromTimestamptz() throws SQLException {
		Time expectedTime = new Time(
				ZonedDateTime.of(1970, 1, 1, 6, 1, 2, 0, UTC_TZ.toZoneId()).toInstant().toEpochMilli());
		Time expectedTimeFromEST = new Time(
				ZonedDateTime.of(1970, 1, 1, 6, 1, 2, 0, UTC_TZ.toZoneId()).toInstant().toEpochMilli());
		inputStream = getInputStreamWithDates();
		resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535);
		resultSet.next();
		assertEquals(expectedTime, resultSet.getTime("timestamptz"));
		assertEquals(expectedTimeFromEST, resultSet.getTime("timestamptz", EST_CALENDAR));
	}

	@Test
	void shouldReturnTimestampFromTimestamptz() throws SQLException {
		Timestamp expectedTimestamp = new Timestamp(
				ZonedDateTime.of(2022, 5, 11, 6, 1, 2, 0, UTC_TZ.toZoneId()).toInstant().toEpochMilli());

		inputStream = getInputStreamWithDates();
		resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535);
		resultSet.next();

		assertEquals(expectedTimestamp, resultSet.getTimestamp("timestamptz"));
		assertEquals(expectedTimestamp, resultSet.getObject("timestamptz"));
		assertEquals(expectedTimestamp, resultSet.getTimestamp("timestamptz", EST_CALENDAR));
	}

	@Test
	void shouldReturnDateFromTimestamptz() throws SQLException {
		Date expectedDate = new Date(
				ZonedDateTime.of(2022, 5, 11, 0, 0, 0, 0, UTC_TZ.toZoneId()).toInstant().toEpochMilli());

		inputStream = getInputStreamWithDates();
		resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535);
		resultSet.next();

		assertEquals(expectedDate, resultSet.getDate("timestamptz"));

		Date expectedDateEST = new Date(
				ZonedDateTime.of(2022, 5, 11, 5, 0, 0, 0, UTC_TZ.toZoneId()).toInstant().toEpochMilli());
		assertEquals(expectedDateEST, resultSet.getDate("timestamptz", EST_CALENDAR));
	}

	@Test
	void shouldReturnNullForTimeTypesWithNullValues() throws SQLException {
		inputStream = getInputStreamWithDates();
		resultSet = new FireboltResultSet(inputStream, "any", "any", 65535);
		resultSet.next();
		resultSet.next();
		assertNull(resultSet.getTimestamp("timestamptz"));
		assertNull(resultSet.getTime("timestamptz"));
		assertNull(resultSet.getDate("timestamptz"));
		assertNull(resultSet.getTimestamp("timestamptz", EST_CALENDAR));
		assertNull(resultSet.getTime("timestamptz", EST_CALENDAR));
		assertNull(resultSet.getDate("timestamptz", EST_CALENDAR));
	}

	@Test
	void shouldReturnDataTypes() throws SQLException {
		inputStream = getInputStreamWithNewTypes();
		resultSet = new FireboltResultSet(inputStream, "any", "any", 65535);
		resultSet.next();
		assertEquals(Types.VARCHAR, resultSet.getMetaData().getColumnType(1));
		assertEquals(Types.DATE, resultSet.getMetaData().getColumnType(2));
		assertEquals(Types.DATE, resultSet.getMetaData().getColumnType(3));
		assertEquals(Types.DATE, resultSet.getMetaData().getColumnType(4));
		assertEquals(Types.TIMESTAMP, resultSet.getMetaData().getColumnType(5));
		assertEquals(Types.TIMESTAMP, resultSet.getMetaData().getColumnType(6));
		assertEquals(Types.TIMESTAMP, resultSet.getMetaData().getColumnType(7));
		assertEquals(Types.TIMESTAMP, resultSet.getMetaData().getColumnType(8));
		assertEquals(Types.ARRAY, resultSet.getMetaData().getColumnType(9));
		assertEquals(Types.NUMERIC, resultSet.getMetaData().getColumnType(10));
		assertEquals(Types.INTEGER, resultSet.getMetaData().getColumnType(11));
		assertEquals(Types.NULL, resultSet.getMetaData().getColumnType(12));
		assertEquals(Types.NULL, resultSet.getMetaData().getColumnType(13));
	}

	@Test
	void shouldReturnDataForNewNonNumericDataTypes() throws SQLException {
		inputStream = getInputStreamWithNewTypes();
		resultSet = new FireboltResultSet(inputStream, "any", "any", 65535);
		resultSet.next();
		assertEquals("text", resultSet.getObject(1));
		assertEquals(Date.valueOf(LocalDate.of(1, 3, 28)), resultSet.getObject(2));
		assertEquals(Date.valueOf(LocalDate.of(1860, 3, 4)), resultSet.getObject(3));
		assertEquals(Date.valueOf(LocalDate.of(1, 1, 1)), resultSet.getObject(4));
		assertEquals(Timestamp.valueOf(LocalDateTime.of(2019, 7, 31, 1, 1, 1, 123400000)), resultSet.getObject(6));
		assertEquals(Timestamp.valueOf(LocalDateTime.of(1111, 1, 5, 17, 4, 42, 123456000)), resultSet.getObject(7));
		assertEquals(Timestamp.valueOf(LocalDateTime.of(1111, 1, 5, 17, 4, 42, 123456000)), resultSet.getObject(8));
		assertArrayEquals(new Integer[] { 1, 2, 3, 4 }, (Integer[]) resultSet.getObject(9));
		assertEquals(new BigDecimal("1231232.123459999990457054844258706536"), resultSet.getObject(10));
		assertNull(resultSet.getObject(11));
		assertNull(resultSet.getObject(12));
		assertNull(resultSet.getObject(13));
	}

	@Test
	void shouldGetObjectsForNumericTypes() throws SQLException {
		inputStream = getInputStreamWithNumericTypes();
		resultSet = new FireboltResultSet(inputStream, "any", "any", 65535);
		resultSet.next();
		assertEquals(1, resultSet.getObject(1, Integer.class));
		assertEquals(new BigInteger("1"), resultSet.getObject(1, BigInteger.class));
		assertEquals(1, resultSet.getObject(1, Long.class));
		assertEquals(30000000000L, resultSet.getObject(2, Long.class));
		assertEquals(new BigInteger("30000000000"), resultSet.getObject(2, BigInteger.class));
		assertEquals(1.23f, resultSet.getObject(3, Float.class));
		assertEquals(new BigDecimal("1.23"), resultSet.getObject(3, BigDecimal.class));
		assertEquals(1.23456789012, resultSet.getObject(4, Double.class));
		assertEquals(new BigDecimal("1.23456789012"), resultSet.getObject(4, BigDecimal.class));
		assertEquals(new BigDecimal("1231232.123459999990457054844258706536"), resultSet.getObject(5, BigDecimal.class));
	}

	@Test
	void shouldReturnDataWithProvidedTypes() throws SQLException {
		inputStream = getInputStreamWithNewTypes();
		resultSet = new FireboltResultSet(inputStream, "any", "any", 65535);
		resultSet.next();
		assertEquals("text", resultSet.getObject(1, String.class));
		assertArrayEquals("text".getBytes(), resultSet.getObject(1, byte[].class));
		assertArrayEquals(new Integer[] { 1, 2, 3, 4 }, ((Integer[]) resultSet.getObject(9, Array.class).getArray()));
		assertEquals(new BigDecimal("1231232.123459999990457054844258706536"),
				resultSet.getObject(10, BigDecimal.class));
		assertNull(resultSet.getObject(11, Integer.class));
		assertNull(resultSet.getObject(12, Object.class));
		assertNull(resultSet.getObject(13, Object.class));
	}

	@Test
	void shouldReturnDataAndTypesForNumericTypes() throws SQLException {
		inputStream = getInputStreamWithNumericTypes();
		resultSet = new FireboltResultSet(inputStream, "any", "any", 65535);
		resultSet.next();
		assertEquals(1, resultSet.getObject(1));
		assertEquals(Types.INTEGER, resultSet.getMetaData().getColumnType(1));
		assertEquals(30000000000L, resultSet.getObject(2));
		assertEquals(Types.BIGINT, resultSet.getMetaData().getColumnType(2));
		assertEquals(1.23f, resultSet.getObject(3));
		assertEquals(Types.REAL, resultSet.getMetaData().getColumnType(3));
		assertEquals(1.23456789012, resultSet.getObject(4));
		assertEquals(Types.DOUBLE, resultSet.getMetaData().getColumnType(4));
		assertEquals(Types.NUMERIC, resultSet.getMetaData().getColumnType(5));
		assertEquals(38, resultSet.getMetaData().getPrecision(5));
		assertEquals(30, resultSet.getMetaData().getScale(5));
		assertEquals(new BigDecimal("1231232.123459999990457054844258706536"), resultSet.getObject(5));
		assertEquals(80000, resultSet.getObject(6));
		assertEquals(Types.INTEGER, resultSet.getMetaData().getColumnType(6));
		assertEquals(30000000000L, resultSet.getObject(7));
		assertEquals(Types.BIGINT, resultSet.getMetaData().getColumnType(7));
		assertEquals(1.23f, resultSet.getObject(8));
		assertEquals(Types.REAL, resultSet.getMetaData().getColumnType(8));
		assertEquals(Types.DOUBLE, resultSet.getMetaData().getColumnType(9));
		assertEquals(1.23456789012, resultSet.getObject(9));
		assertEquals(Types.NUMERIC, resultSet.getMetaData().getColumnType(10));
		assertEquals(new BigDecimal("1231232.123459999990457054844258706536"), resultSet.getObject(10));
	}

	@Test
	void shouldReturnDateTimeObjectsWithProvidedTypes() throws SQLException {
		inputStream = getInputStreamWithNewTypes();
		resultSet = new FireboltResultSet(inputStream, "any", "any", 65535);
		resultSet.next();
		assertEquals(Date.valueOf(LocalDate.of(1, 3, 28)), resultSet.getObject(2, Date.class));
		assertEquals(Date.valueOf(LocalDate.of(1860, 3, 4)), resultSet.getObject(3, Date.class));
		assertEquals(Date.valueOf(LocalDate.of(1, 1, 1)), resultSet.getObject(4, Date.class));
		assertEquals(Timestamp.valueOf(LocalDateTime.of(2019, 7, 31, 1, 1, 1, 123400000)),
				resultSet.getObject(6, Timestamp.class));
		assertEquals(LocalDateTime.of(2019, 7, 31, 1, 1, 1, 123400000).atOffset(ZoneOffset.of("+00:00")),
				resultSet.getObject(6, OffsetDateTime.class));
		assertEquals(Timestamp.valueOf(LocalDateTime.of(1111, 1, 5, 17, 4, 42, 123456000)),
				resultSet.getObject(7, Timestamp.class));
		assertEquals(LocalDateTime.of(1111, 1, 5, 17, 4, 42, 123456000).atOffset(ZoneOffset.of("+00:00")),
				resultSet.getObject(7, OffsetDateTime.class));
		assertEquals(Timestamp.valueOf(LocalDateTime.of(1111, 1, 5, 17, 4, 42, 123456000)),
				resultSet.getObject(8, Timestamp.class));
		assertEquals(LocalDateTime.of(1111, 1, 5, 17, 4, 42, 123456000).atOffset(ZoneOffset.of("+00:00")),
				resultSet.getObject(8, OffsetDateTime.class));
		assertArrayEquals(new Integer[] { 1, 2, 3, 4 }, ((Integer[]) resultSet.getObject(9, Array.class).getArray()));
	}

	@Test
	void shouldThrowExceptionWhenConvertingIncompatibleTypes() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535);
		resultSet.next();
		FireboltException exception = assertThrows(FireboltException.class, () -> resultSet.getObject(1, String.class));
		assertEquals("conversion to class java.lang.String from java.lang.Integer not supported", exception.getMessage());
	}

	@Test
	void shouldThrowExceptionWhenConvertingUnsupportedTypes() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = new FireboltResultSet(inputStream, "any_name", "array_db", 65535);
		resultSet.next();
		FireboltException exception = assertThrows(FireboltException.class, () -> resultSet.getObject(1, TimeZone.class));
		assertEquals("conversion to java.util.TimeZone from java.lang.Integer not supported", exception.getMessage());
	}

	@Test
	void shouldConvertInt64s() throws SQLException {
		inputStream = getInputStreamWithBigInt64();
		resultSet = new FireboltResultSet(inputStream);
		resultSet.next();
		assertEquals(new BigInteger("18446744073709551615"), resultSet.getObject(1, BigInteger.class));
		assertEquals(new BigInteger("-9223372036854775807"), resultSet.getObject(2, BigInteger.class));

	}

	private InputStream getInputStreamWithCommonResponseExample() {
		return FireboltResultSetTest.class.getResourceAsStream("/responses/firebolt-response-example");
	}

	private InputStream getInputStreamWithEmpty() {
		return FireboltResultSetTest.class.getResourceAsStream("/responses/firebolt-response-with-empty");
	}

	private InputStream getInputStreamWithNulls() {
		return FireboltResultSetTest.class.getResourceAsStream("/responses/firebolt-response-with-nulls");
	}

	private InputStream getInputStreamWitExplain() {
		return FireboltResultSetTest.class.getResourceAsStream("/responses/firebolt-response-with-escape-characters");
	}

	private InputStream getInputStreamWithByteA() {
		return FireboltResultSetTest.class.getResourceAsStream("/responses/firebolt-response-with-bytea");
	}

	private InputStream getInputStreamWithBooleans() {
		return FireboltResultSetTest.class.getResourceAsStream("/responses/firebolt-response-with-booleans");
	}

	private InputStream getInputStreamWithDates() {
		return FireboltResultSetTest.class.getResourceAsStream("/responses/firebolt-response-with-dates");
	}

	private InputStream getInputStreamWithBigInt64() {
		return FireboltResultSetTest.class.getResourceAsStream("/responses/firebolt-response-with-bigint-64");
	}

	private InputStream getInputStreamWithNewTypes() {
		return FireboltResultSetTest.class.getResourceAsStream("/responses/firebolt-response-with-new-types");
	}

	private InputStream getInputStreamWithNumericTypes() {
		return FireboltResultSetTest.class.getResourceAsStream("/responses/firebolt-response-with-numeric-types.csv");
	}

}
