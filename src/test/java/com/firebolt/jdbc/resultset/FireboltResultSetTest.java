package com.firebolt.jdbc.resultset;

import static com.firebolt.jdbc.exception.ExceptionType.TYPE_TRANSFORMATION_ERROR;
import static java.lang.String.format;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Array;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.sql.Wrapper;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.Callable;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterEach;
//import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.junitpioneer.jupiter.DefaultTimeZone;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import com.firebolt.jdbc.CheckedFunction;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.statement.FireboltStatement;
import com.firebolt.jdbc.util.LoggerUtil;

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

//	@Test
	void shouldReturnMetadata() throws SQLException {
		// This only tests that Metadata is available with the resultSet.
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		assertNotNull(resultSet.getMetaData());
		assertEquals("a_table", resultSet.getMetaData().getTableName(1));
		assertEquals("a_db", resultSet.getMetaData().getCatalogName(1));
	}

//	@Test
	void attributes() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		assertEquals(ResultSet.CONCUR_READ_ONLY, resultSet.getConcurrency());
		assertEquals(ResultSet.FETCH_FORWARD, resultSet.getFetchDirection());
		assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, resultSet.getHoldability());
	}

//	@Test
	void setFetchDirection() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		resultSet.setFetchDirection(ResultSet.FETCH_FORWARD); // should just work
		assertThrows(SQLException.class, () -> resultSet.setFetchDirection(ResultSet.FETCH_REVERSE));
		assertThrows(SQLException.class, () -> resultSet.setFetchDirection(ResultSet.FETCH_UNKNOWN));
	}

//	@Test
	void getRow() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		int i = 0;
		do {
			assertEquals(i, resultSet.getRow());
			i++;
		} while (resultSet.next());
	}

//	@Test
	void getStatement() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		assertEquals(fireboltStatement, resultSet.getStatement());
	}

//	@Test
	void unsupportedNavigation() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);

		assertThrowsForwardOnly("first", () -> resultSet.first());
		assertThrowsForwardOnly("last", () -> resultSet.last());
		assertThrowsForwardOnly("beforeFirst", () -> {resultSet.beforeFirst(); return null;});
		assertThrowsForwardOnly("afterLast", () -> {resultSet.afterLast(); return null;});
		assertThrowsForwardOnly("absolute", () -> resultSet.absolute(1));
		assertThrowsForwardOnly("relative", () -> resultSet.relative(1));
		assertThrowsForwardOnly("previous", () -> resultSet.previous());
	}

	private void assertThrowsForwardOnly(String name, Callable<?> method) {
		assertEquals(format("Cannot call %s() for ResultSet of type TYPE_FORWARD_ONLY", name), assertThrows(SQLException.class, method::call).getMessage());
	}

//	@Test
	void unsupported() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);

		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getCursorName());
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.rowUpdated());
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.rowDeleted());
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.rowInserted());
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getRowId(1));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getRowId("no-name"));

		// updates
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNull(1));

		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBoolean(1, true));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateByte(1, (byte)0));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateShort(1, (short)0));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateInt(1, 0));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateLong(1, 0));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateFloat(1, 0.0f));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateDouble(1, 0.0));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBigDecimal(1, new BigDecimal(0)));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateString(1, ""));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBytes(1, new byte[0]));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateDate(1, new Date(0)));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateTime(1, new Time(0)));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateTimestamp(1, new Timestamp(0)));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateAsciiStream(1, new ByteArrayInputStream(new byte[0]), 0));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateAsciiStream(1, new ByteArrayInputStream(new byte[0]), 0L));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBinaryStream(1, new ByteArrayInputStream(new byte[0]), 0));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBinaryStream(1, new ByteArrayInputStream(new byte[0]), 0L));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateCharacterStream(1, new StringReader(""), 0));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateCharacterStream(1, new StringReader(""), 0L));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateObject(1, null, 0));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateObject(1, null));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNull("label"));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBoolean("label", true));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateByte("label", (byte)0));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateShort("label", (short)0));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateInt("label", 0));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateLong("label", 0));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateFloat("label", 0.0f));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateDouble("label", 0.0));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBigDecimal("label", new BigDecimal(0)));

		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateString("label", ""));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBytes("label", new byte[0]));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateDate("label", new Date(0)));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateTime("label", new Time(0)));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateTimestamp("label", new Timestamp(0)));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateAsciiStream("label", new ByteArrayInputStream(new byte[0]), 0));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateAsciiStream("label", new ByteArrayInputStream(new byte[0]), 0L));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBinaryStream("label", new ByteArrayInputStream(new byte[0]), 0));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBinaryStream("label", new ByteArrayInputStream(new byte[0]), 0L));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateCharacterStream("label", new StringReader(""), 0));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateObject("label", null, 0));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateObject("label", null));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.insertRow());
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateRow());
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.deleteRow());
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.refreshRow());
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.cancelRowUpdates());
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.moveToInsertRow());
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.moveToCurrentRow());
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateRef(1, null));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateRef("label", null));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBlob(1, mock(java.sql.Blob.class)));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBlob("label", mock(java.sql.Blob.class)));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateClob(1, mock(Clob.class)));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateClob("label", mock(Clob.class)));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateArray(1, mock(Array.class)));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateArray("label", mock(Array.class)));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateRowId(1, mock(RowId.class)));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateRowId("label", mock(RowId.class)));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNString(1, ""));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNString("label", ""));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNClob(1, mock(java.sql.NClob.class)));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNClob("label", mock(java.sql.NClob.class)));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateSQLXML(1, mock(SQLXML.class)));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateSQLXML("", mock(SQLXML.class)));

		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNCharacterStream(1, new StringReader(""), 0));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNCharacterStream("label", new StringReader(""), 0));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateCharacterStream(1, new StringReader(""), 0));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateCharacterStream(1, new StringReader(""), 0L));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateAsciiStream("label", new ByteArrayInputStream(new byte[0]), 0));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateAsciiStream("label", new ByteArrayInputStream(new byte[0]), 0L));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBinaryStream("label", new ByteArrayInputStream(new byte[0]), 0));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBinaryStream("label", new ByteArrayInputStream(new byte[0]), 0L));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateCharacterStream("label", new StringReader(""), 0));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateCharacterStream("label", new StringReader(""), 0L));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBlob(1, new ByteArrayInputStream(new byte[0]), 0));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBlob("label", new ByteArrayInputStream(new byte[0]), 0L));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBlob(1, new ByteArrayInputStream(new byte[0]), 0));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBlob(1, new ByteArrayInputStream(new byte[0]), 0L));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateClob(1, new StringReader(""), 0));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateClob(1, new StringReader(""), 0L));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateClob("label", new StringReader(""), 0));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateClob("label", new StringReader(""), 0L));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNClob(1, new StringReader(""), 0));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNClob(1, new StringReader(""), 0L));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNClob("label", new StringReader(""), 0));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNClob("label", new StringReader(""), 0L));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNCharacterStream(1, new StringReader("")));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNCharacterStream("label", new StringReader("")));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateAsciiStream(1, new ByteArrayInputStream(new byte[0])));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBinaryStream(1, new ByteArrayInputStream(new byte[0])));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateCharacterStream(1, new StringReader("")));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateAsciiStream("label", new ByteArrayInputStream(new byte[0])));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBinaryStream("label", new ByteArrayInputStream(new byte[0])));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateCharacterStream("label", new StringReader("")));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBlob(1, new ByteArrayInputStream(new byte[0])));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBlob("label", new ByteArrayInputStream(new byte[0])));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateClob(1, new StringReader("")));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateClob("label", new StringReader("")));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNClob(1, new StringReader("")));
		assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNClob("label", new StringReader("")));
	}

//	@Test
	void fetchSize() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		assertEquals(0, resultSet.getFetchSize());

		resultSet.setFetchSize(0); // ignored
		resultSet.setFetchSize(1); // ignored
		assertThrows(SQLException.class, () -> resultSet.setFetchSize(-1));
	}

//	@Test
	void shouldNotBeLastWhenThereIsMoreData() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		assertFalse(resultSet.isLast());
	}

//	@Test
	void shouldNotBeLastAtLastLine() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		for (int i = 0; i < 5; i++) {
			assertTrue(resultSet.next());
			if (i == 0) {
				String typeText = resultSet.getString(7);
				assertNull(typeText);
			}
		}
		assertFalse(resultSet.next());
	}

//	@Test
	void maxRows() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		when(fireboltStatement.getMaxRows()).thenReturn(1);
		resultSet = createResultSet(inputStream);
		assertTrue(resultSet.next());
		assertFalse(resultSet.next()); // the result has 2 rows but maxRows=1, so the second next() returns false
	}

//	@Test
	void shouldReadAllTheData() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
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

//	@Test
	void shouldBeBeforeFirstIfFirstRowNotRead() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		assertTrue(resultSet.isBeforeFirst());
		resultSet.next();
		assertFalse(resultSet.isBeforeFirst());
	}

//	@Test
	void shouldGetBigDecimalSimple() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		assertEquals(new BigDecimal("1"), resultSet.getBigDecimal(1));
		assertEquals(new BigDecimal("1"), resultSet.getBigDecimal("id"));
	}

//	@Test
	@SuppressWarnings("deprecation") // ResultSet.getBigDecimal() is deprecated but  still has to be tested.
	void shouldGetBigDecimalWithScale() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		assertEquals(new BigDecimal("1").setScale(2, RoundingMode.HALF_UP), resultSet.getBigDecimal(1, 2));
		assertEquals(new BigDecimal("1").setScale(2, RoundingMode.HALF_UP), resultSet.getBigDecimal("id", 2));
	}

//	@Test
	void shouldGetBigDecimalNull() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		resultSet.next();
		assertNull(resultSet.getBigDecimal(7));
		assertNull(resultSet.getBigDecimal("an_integer"));
	}

//	@Test
	void shouldBeFirstWhenNextRecordIsTheFirstToRead() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		assertTrue(resultSet.isFirst());
		resultSet.next();
		assertFalse(resultSet.isFirst());
	}

//	@Test
	void shouldBeAfterReadingTheLast() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		assertFalse(resultSet.isAfterLast());
		assertFalse(resultSet.isLast());
		while (resultSet.next()) {
			// just read everything
			assertFalse(resultSet.isAfterLast());
		}
		assertTrue(resultSet.isAfterLast());
		assertFalse(resultSet.isLast());
	}

//	@Test
	void shouldReturnFalseWhenCallingWasNullAfterRead() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		resultSet.getInt(1);
		assertFalse(resultSet.wasNull());
	}

//	@Test
	void shouldThrowExceptionWhenCallingWasNullBeforeAnyGet() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		assertThrows(IllegalArgumentException.class, () -> resultSet.wasNull(),
				"A column must be read before checking nullability");
	}

//	@Test
	void shouldReturnTrueWhenLastValueGotWasNull() throws SQLException {
		// This only tests that Metadata is available with the resultSet.
		inputStream = getInputStreamWithNulls();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		resultSet.getObject(2);
		assertTrue(resultSet.wasNull());
		resultSet.next();
		assertTrue(resultSet.wasNull());
	}

//	@Test
	void shouldThrowIfTypeIsNull() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		assertEquals("The type provided is null", assertThrows(SQLException.class, () -> resultSet.getObject(1, (Class<?>)null)).getMessage());
	}

//	@Test
	void shouldReturnInt() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		assertEquals(1, resultSet.getInt(1));
		assertEquals(1, resultSet.getInt("id"));
		assertEquals(1, resultSet.getObject(1, Long.class));
		assertEquals(1L, resultSet.getObject(1, Map.of("int", Long.class)));
		assertEquals(1L, resultSet.getObject("id", Map.of("INTEGER", Long.class)));
		assertEquals(1., resultSet.getObject(1, Map.of("int32", Double.class)));
		assertThrows(SQLException.class, () -> resultSet.getObject(1, Map.of("real", Double.class))); // exising type that does not match column type
		assertThrows(SQLException.class, () -> resultSet.getObject(1, Map.of("notatype", Double.class))); // type alias that does not exist

		resultSet.next();
		assertEquals(2, resultSet.getInt(1));
		assertEquals(2, resultSet.getInt("id"));
		assertEquals(2, resultSet.getObject(1, Long.class));

		assertEquals(2, resultSet.getLong(1));
		assertEquals(2, resultSet.getLong("id"));
	}

//	@Test
	void shouldReturnFloat() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		assertEquals(14.6f, resultSet.getFloat(6));
		assertEquals(14.6f, resultSet.getFloat("a_double"));
		assertEquals(14.6f, resultSet.getObject(6, Float.class));
		assertEquals(14.6, resultSet.getObject(6, Map.of("Float32", Double.class)));
		assertEquals((short)14, resultSet.getObject(6, Map.of("Float32", Short.class)));

		resultSet.next();
		assertEquals(0, resultSet.getFloat(6));
		assertEquals(0, resultSet.getFloat("a_double"));
		assertNull(resultSet.getObject(6, Float.class));
	}

//	@Test
	void shouldReturnDouble() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		assertEquals(14.6d, resultSet.getDouble(6));
		assertEquals(14.6d, resultSet.getDouble("a_double"));
		resultSet.next();
		assertEquals(0, resultSet.getDouble(6));
		assertEquals(0, resultSet.getDouble("a_double"));
	}

//	@Test
	void shouldReturnString() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		String expected = "Taylor's Prime Steak House";
		assertEquals(expected, resultSet.getString(3));
		assertEquals(expected, resultSet.getString("name"));
		assertEquals(expected, resultSet.getObject(3, String.class));
		assertEquals(expected, resultSet.getNString(3));
		assertEquals(expected, resultSet.getNString("name"));
		assertEquals(expected, getStringFromClob(resultSet.getClob(3)));
		assertEquals(expected, getStringFromClob(resultSet.getClob("name")));
		assertEquals(expected, getStringFromClob(resultSet.getNClob(3)));
		assertEquals(expected, getStringFromClob(resultSet.getNClob("name")));
	}

	private String getStringFromClob(Clob clob) throws SQLException {
		return clob.getSubString(1, (int)clob.length());
	}

//	@Test
	void shouldReturnShort() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		assertEquals(5, resultSet.getShort("an_integer"));
		assertEquals(5, resultSet.getShort(7));
		resultSet.next();
		assertEquals(0, resultSet.getShort("an_integer"));
		assertEquals(0, resultSet.getShort(7));
	}

//	@Test
	void shouldReturnTypeForward() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		assertEquals(TYPE_FORWARD_ONLY, resultSet.getType());
	}

//	@Test
	void shouldReturnBytes() throws SQLException, IOException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		byte[] expected = "Taylor\\'s Prime Steak House".getBytes();
		assertArrayEquals(expected, resultSet.getBytes(3));
		assertArrayEquals(expected, resultSet.getBytes("name"));
		assertArrayEquals(expected, resultSet.getBinaryStream(3).readAllBytes());
		assertArrayEquals(expected, resultSet.getBinaryStream("name").readAllBytes());
		assertArrayEquals(expected, resultSet.getObject(3, byte[].class));
		assertArrayEquals(expected, resultSet.getObject("name", byte[].class));
		assertArrayEquals(expected, resultSet.getBlob(3).getBinaryStream().readAllBytes());
		assertArrayEquals(expected, resultSet.getBlob("name").getBinaryStream().readAllBytes());
		resultSet.next();
		assertNull(resultSet.getBytes(3));
		assertNull(resultSet.getBlob(3));
	}

//	@Test
	void shouldReturnNullWhenValueIsNull() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		resultSet.next();
		assertNull(resultSet.getBytes(3));
		assertNull(resultSet.getBlob(3));
	}

//	@Test
	void shouldReturnByte() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		assertEquals(1, resultSet.getByte(1));
		assertEquals(1, resultSet.getByte("id"));
	}

//	@Test
	void shouldReturn0ByteWhenValueIsNull() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		resultSet.next();
		assertEquals((byte) 0, resultSet.getByte(3));
	}

//	@Test
	void shouldReturnNullWhenValueStringIsNull() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		resultSet.next(); // second line contains \N which represents a null value
		assertNull(resultSet.getString(3));
		assertNull(resultSet.getString("name"));
		assertNull(resultSet.getNString(3));
		assertNull(resultSet.getNString("name"));
		assertNull(resultSet.getClob(3));
		assertNull(resultSet.getClob("name"));
		assertNull(resultSet.getNClob(3));
		assertNull(resultSet.getNClob("name"));
	}

//	@Test
	void shouldReturnDate() throws SQLException {
		Date expectedDate = Date.valueOf(LocalDate.of(2022, 5, 10));
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		resultSet.next();

		assertEquals(expectedDate, resultSet.getDate(4));
		assertEquals(expectedDate, resultSet.getDate("a_date"));
	}

//	@Test
	void shouldReturnTimeStamp() throws SQLException, ParseException {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		java.util.Date parsedDate = dateFormat.parse("2022-05-10 13:01:02");
		Timestamp timestamp = new Timestamp(parsedDate.getTime());

		inputStream = getInputStreamWithDates();
		resultSet = createResultSet(inputStream);
		resultSet.next();

		assertEquals(timestamp, resultSet.getTimestamp(2));
		assertEquals(timestamp, resultSet.getTimestamp("a_datetime"));
	}

//	@Test
	void shouldReturnBoolean() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		resultSet.next();

		assertTrue(resultSet.getBoolean(5));
		assertTrue(resultSet.getBoolean("is_online"));
		resultSet.next();
		assertFalse(resultSet.getBoolean(5));
		assertFalse(resultSet.getBoolean("is_online"));
	}

//	@Test
	void shouldReturnTime() throws SQLException {
		Time expectedTime = new Time(
				ZonedDateTime.of(1970, 1, 1, 13, 1, 2, 0, UTC_TZ.toZoneId()).toInstant().toEpochMilli());
		inputStream = getInputStreamWithDates();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		assertEquals(expectedTime, resultSet.getTime(2));
		assertEquals(expectedTime, resultSet.getTime("a_datetime"));
	}

//	@Test
	void shouldGetArray() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		String[][][] firstArray = { { { "1", "2" }, { "3", "4" } } };
		assertArrayEquals(firstArray, ((String[][][]) (resultSet.getArray("arr")).getArray()));
		assertArrayEquals(firstArray, ((String[][][]) (resultSet.getArray(2)).getArray()));
	}

//	@Test
	void shouldReturnUnescapedString() throws SQLException {
		String expected = "[0] [Aggregate] GroupBy: [] Aggregates: [COUNT(DISTINCT FB_NODE_2.a1), APPROX_COUNT_DISTINCT(FB_NODE_2.a1)] @ FB_NODE_1\n \\_[1] [StoredTable] Name: 'ft', used 1/1 column(s) FACT @ FB_NODE_2\n";
		inputStream = getInputStreamWitExplain();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		assertEquals(expected, resultSet.getString(1));
		assertEquals(expected, resultSet.getObject(1));
	}

//	@Test
	void shouldThrowException() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		resultSet.next(); // second line contains \N which represents a null value
		assertNull(resultSet.getString(3));
		assertNull(resultSet.getString("name"));
	}

//	@Test
	void shouldThrowExceptionWhenCheckingWasNullAfterClose() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		resultSet.close();
		assertThrows(SQLException.class, resultSet::wasNull);
	}

//	@Test
	void shouldThrowExceptionWhenGettingValueAfterClose() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		resultSet.close();
		assertThrows(SQLException.class, () -> resultSet.getObject(1));
	}

//	@Test
	void shouldThrowSQLExceptionWhenGettingValueWithInvalidColumnIndex() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		resultSet.close();
		assertThrows(SQLException.class, () -> resultSet.getObject("INVALID_COLUMN"));
	}

//	@Test
	void shouldCloseStream() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		assertFalse(resultSet.isClosed());
		resultSet.close();
		assertTrue(resultSet.isClosed());
	}

//	@Test
	void shouldCloseStatementWhenCloseOnCompletion() throws SQLException {
		when(fireboltStatement.isCloseOnCompletion()).thenReturn(true);
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		resultSet.close();
		verify(fireboltStatement).close();
	}

//	@Test
	void shouldNotCloseStatementWhenNotCloseOnCompletion() throws SQLException {
		when(fireboltStatement.isCloseOnCompletion()).thenReturn(false);
		inputStream = getInputStreamWithCommonResponseExample();
		when(fireboltStatement.getMaxRows()).thenReturn(1024);
		when(fireboltStatement.getMaxFieldSize()).thenReturn(0);
		resultSet = createResultSet(inputStream);
		resultSet.close();
		verifyNoMoreInteractions(fireboltStatement);
	}

//	@Test
	void shouldNotThrowExceptionWhenClosingTwice() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		resultSet.close();
		assertTrue(resultSet.isClosed());
		try {
			resultSet.close();
		} catch (Exception e) {
			fail();
		}
	}

//	@Test
	void shouldThrowExceptionWhenColumnDoesNotExist() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		assertThrows(SQLException.class, () -> resultSet.getObject(50));
	}

//	@Test
	void shouldReturnEmptyWhenValueFoundIsEmpty() throws SQLException {
		inputStream = getInputStreamWithEmpty();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		assertEquals("", resultSet.getObject("name"));
		assertEquals("", resultSet.getObject("city"));
	}

//	@Test
	void shouldLogResultSet() throws SQLException {
		// This only tests that Metadata is available with the resultSet.
		inputStream = getInputStreamWithCommonResponseExample();
		try (MockedStatic<LoggerUtil> loggerUtilMockedStatic = mockStatic(LoggerUtil.class)) {
			loggerUtilMockedStatic.when(() -> LoggerUtil.logInputStream(inputStream)).thenReturn(inputStream);
			resultSet = createResultSet(inputStream);
			loggerUtilMockedStatic.verify(() -> LoggerUtil.logInputStream(inputStream));
		}
	}

//	@Test
	void shouldGetTimeWithTimezoneFromCalendar() throws SQLException {
		inputStream = getInputStreamWithDates();
		try (MockedStatic<LoggerUtil> loggerUtilMockedStatic = mockStatic(LoggerUtil.class)) {
			loggerUtilMockedStatic.when(() -> LoggerUtil.logInputStream(inputStream)).thenReturn(inputStream);
			resultSet = createResultSet(inputStream);
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

//	@Test
	void shouldGetTimestampWithTimezoneFromCalendar() throws SQLException {
		inputStream = getInputStreamWithDates();
		try (MockedStatic<LoggerUtil> loggerUtilMockedStatic = mockStatic(LoggerUtil.class)) {
			loggerUtilMockedStatic.when(() -> LoggerUtil.logInputStream(inputStream)).thenReturn(inputStream);
			resultSet = createResultSet(inputStream);
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

//	@Test
	void shouldGetTimeObjectsWithTimeZoneFromResponse() throws SQLException {
		inputStream = getInputStreamWithDates();
		try (MockedStatic<LoggerUtil> loggerUtilMockedStatic = mockStatic(LoggerUtil.class)) {
			loggerUtilMockedStatic.when(() -> LoggerUtil.logInputStream(inputStream)).thenReturn(inputStream);
			resultSet = createResultSet(inputStream);
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

//	@Test
	void shouldGetDateWithTimezoneFromCalendar() throws SQLException {
		inputStream = getInputStreamWithDates();
		try (MockedStatic<LoggerUtil> loggerUtilMockedStatic = mockStatic(LoggerUtil.class)) {
			loggerUtilMockedStatic.when(() -> LoggerUtil.logInputStream(inputStream)).thenReturn(inputStream);
			resultSet = createResultSet(inputStream);
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

//	@Test
	void shouldFindNullByteA() throws SQLException {
		inputStream = getInputStreamWithByteA();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		assertNull(resultSet.getObject("null_bytea"));
	}

//	@Test
	void shouldFindByteAWithValue() throws SQLException {
		inputStream = getInputStreamWithByteA();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		assertArrayEquals(new byte[] { -34, -83, -66, -17 }, (byte[]) resultSet.getObject("a_bytea"));
		assertEquals("\\xdeadbeef", resultSet.getString("a_bytea"));
		resultSet.next();
		assertArrayEquals(new byte[] { 0, -85 }, resultSet.getObject("a_bytea", byte[].class));
		assertEquals("\\x00ab", resultSet.getString("a_bytea"));
	}

//	@Test
	void shouldFindEmptyByteA() throws SQLException {
		inputStream = getInputStreamWithByteA();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		assertArrayEquals(new byte[] {}, (byte[]) resultSet.getObject("an_empty_bytea"));
		assertEquals("", resultSet.getString("an_empty_bytea"));
	}

//	@Test
	void shouldThrowExceptionWhenCannotConvertToByteArray() throws SQLException {
		inputStream = getInputStreamWithByteA();
		resultSet = createResultSet(inputStream);
		while (resultSet.next()) {
			assertEquals("Cannot convert binary string in non-hex format to byte array", assertThrows(SQLException.class, () -> resultSet.getObject("false_bytea")).getMessage());
		}
	}

//	@Test
	void shouldReturnTrueWhenBooleanFoundIsTrue() throws SQLException {
		inputStream = getInputStreamWithBooleans();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		assertTrue((Boolean) resultSet.getObject("true_boolean"));
		assertTrue(resultSet.getBoolean("true_boolean"));
		assertTrue(resultSet.getObject("true_boolean", Boolean.class));
		resultSet.next();
		assertTrue((Boolean) resultSet.getObject("true_boolean"));
		assertTrue(resultSet.getBoolean("true_boolean"));
	}

//	@Test
	void shouldReturnFalseWhenBooleanFoundIsFalse() throws SQLException {
		inputStream = getInputStreamWithBooleans();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		assertFalse((Boolean) resultSet.getObject("false_boolean"));
		assertFalse(resultSet.getBoolean("false_boolean"));
		assertFalse(resultSet.getObject("false_boolean", Boolean.class));
		resultSet.next();
		assertFalse((Boolean) resultSet.getObject("false_boolean"));
		assertFalse(resultSet.getBoolean("false_boolean"));
	}

//	@Test
	void shouldReturnFalseWhenBooleanFoundIsNull() throws SQLException {
		inputStream = getInputStreamWithBooleans();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		assertNull(resultSet.getObject("null_boolean"));
		assertFalse(resultSet.getBoolean("null_boolean"));
	}

//	@Test
	void shouldThrowExceptionWhenBooleanValueIsInvalid() throws SQLException {
		inputStream = getInputStreamWithBooleans();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		assertThrows(FireboltException.class, () -> resultSet.getObject("invalid_boolean"));
	}

//	@Test
	void shouldReturnTimestampFromTimestampntz() throws SQLException {
		Timestamp expectedTimestamp = new Timestamp(
				ZonedDateTime.of(2022, 5, 10, 23, 1, 2, 0, UTC_TZ.toZoneId()).toInstant().toEpochMilli());

		inputStream = getInputStreamWithDates();
		resultSet = createResultSet(inputStream);
		resultSet.next();

		assertEquals(expectedTimestamp, resultSet.getTimestamp("timestampntz"));
		assertEquals(expectedTimestamp, resultSet.getObject("timestampntz"));
		Timestamp expectedTimestampWithDifferentTz = new Timestamp(
				ZonedDateTime.of(2022, 5, 11, 4, 1, 2, 0, UTC_TZ.toZoneId()).toInstant().toEpochMilli());
		assertEquals(expectedTimestampWithDifferentTz, resultSet.getTimestamp("timestampntz", EST_CALENDAR));
	}

//	@Test
	void shouldReturnDateFromTimestampntz() throws SQLException {
		Date expectedDate = new Date(
				ZonedDateTime.of(2022, 5, 10, 0, 0, 0, 0, UTC_TZ.toZoneId()).toInstant().toEpochMilli());

		inputStream = getInputStreamWithDates();
		resultSet = createResultSet(inputStream);
		resultSet.next();

		assertEquals(expectedDate, resultSet.getDate("timestampntz"));

		Date expectedDateEST = new Date(
				ZonedDateTime.of(2022, 5, 10, 5, 0, 0, 0, UTC_TZ.toZoneId()).toInstant().toEpochMilli());
		assertEquals(expectedDateEST, resultSet.getDate("timestampntz", EST_CALENDAR));
	}

//	@Test
	void shouldReturnTimeFromTimestampntz() throws SQLException {
		Time expectedTime = new Time(
				ZonedDateTime.of(1970, 1, 1, 23, 1, 2, 0, UTC_TZ.toZoneId()).toInstant().toEpochMilli());
		Time expectedTimeFromEST = new Time(
				ZonedDateTime.of(1970, 1, 2, 4, 1, 2, 0, UTC_TZ.toZoneId()).toInstant().toEpochMilli());
		inputStream = getInputStreamWithDates();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		assertEquals(expectedTime, resultSet.getTime("timestampntz"));
		assertEquals(expectedTimeFromEST, resultSet.getTime("timestampntz", EST_CALENDAR));
	}

//	@Test
	void shouldReturnTimeFromTimestamptz() throws SQLException {
		Time expectedTime = new Time(
				ZonedDateTime.of(1970, 1, 1, 6, 1, 2, 0, UTC_TZ.toZoneId()).toInstant().toEpochMilli());
		Time expectedTimeFromEST = new Time(
				ZonedDateTime.of(1970, 1, 1, 6, 1, 2, 0, UTC_TZ.toZoneId()).toInstant().toEpochMilli());
		inputStream = getInputStreamWithDates();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		assertEquals(expectedTime, resultSet.getTime("timestamptz"));
		assertEquals(expectedTimeFromEST, resultSet.getTime("timestamptz", EST_CALENDAR));
	}

//	@Test
	void shouldReturnTimestampFromTimestamptz() throws SQLException {
		Timestamp expectedTimestamp = new Timestamp(
				ZonedDateTime.of(2022, 5, 11, 6, 1, 2, 0, UTC_TZ.toZoneId()).toInstant().toEpochMilli());

		inputStream = getInputStreamWithDates();
		resultSet = createResultSet(inputStream);
		resultSet.next();

		assertEquals(expectedTimestamp, resultSet.getTimestamp("timestamptz"));
		assertEquals(expectedTimestamp, resultSet.getObject("timestamptz"));
		assertEquals(expectedTimestamp, resultSet.getTimestamp("timestamptz", EST_CALENDAR));
	}

//	@Test
	void shouldReturnDateFromTimestamptz() throws SQLException {
		Date expectedDate = new Date(
				ZonedDateTime.of(2022, 5, 11, 0, 0, 0, 0, UTC_TZ.toZoneId()).toInstant().toEpochMilli());

		inputStream = getInputStreamWithDates();
		resultSet = createResultSet(inputStream);
		resultSet.next();

		assertEquals(expectedDate, resultSet.getDate("timestamptz"));

		Date expectedDateEST = new Date(
				ZonedDateTime.of(2022, 5, 11, 5, 0, 0, 0, UTC_TZ.toZoneId()).toInstant().toEpochMilli());
		assertEquals(expectedDateEST, resultSet.getDate("timestamptz", EST_CALENDAR));
	}

//	@Test
	void shouldReturnNullForTimeTypesWithNullValues() throws SQLException {
		inputStream = getInputStreamWithDates();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		resultSet.next();
		assertNull(resultSet.getTimestamp("timestamptz"));
		assertNull(resultSet.getTime("timestamptz"));
		assertNull(resultSet.getDate("timestamptz"));
		assertNull(resultSet.getTimestamp("timestamptz", EST_CALENDAR));
		assertNull(resultSet.getTime("timestamptz", EST_CALENDAR));
		assertNull(resultSet.getDate("timestamptz", EST_CALENDAR));
	}

//	@Test
	void shouldReturnDataTypes() throws SQLException {
		inputStream = getInputStreamWithNewTypes();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		assertEquals(Types.VARCHAR, resultSet.getMetaData().getColumnType(1));
		assertEquals(Types.DATE, resultSet.getMetaData().getColumnType(2));
		assertEquals(Types.DATE, resultSet.getMetaData().getColumnType(3));
		assertEquals(Types.DATE, resultSet.getMetaData().getColumnType(4));
		assertEquals(Types.TIMESTAMP, resultSet.getMetaData().getColumnType(5));
		assertEquals(Types.TIMESTAMP, resultSet.getMetaData().getColumnType(6));
		assertEquals(Types.TIMESTAMP, resultSet.getMetaData().getColumnType(7));
		assertEquals(Types.TIMESTAMP_WITH_TIMEZONE, resultSet.getMetaData().getColumnType(8));
		assertEquals(Types.ARRAY, resultSet.getMetaData().getColumnType(9));
		assertEquals(Types.NUMERIC, resultSet.getMetaData().getColumnType(10));
		assertEquals(Types.INTEGER, resultSet.getMetaData().getColumnType(11));
		assertEquals(Types.NULL, resultSet.getMetaData().getColumnType(12));
		assertEquals(Types.NULL, resultSet.getMetaData().getColumnType(13));
	}

//	@Test
	void shouldReturnDataForNewNonNumericDataTypes() throws SQLException {
		inputStream = getInputStreamWithNewTypes();
		resultSet = createResultSet(inputStream);
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

//	@Test
	void shouldGetByte() throws SQLException {
		inputStream = getInputStreamWithNumericTypes();
		resultSet = createResultSet(inputStream);
		resultSet.next();

		assertEquals((byte)1, resultSet.getObject(1, Byte.class));
		assertEquals((byte)1, resultSet.getByte(1));

		assertTransformationError(2, Byte.class);
		assertTransformationError(2, i -> resultSet.getByte(i));

		assertEquals((byte)1, resultSet.getObject(3, Byte.class));
		assertEquals((byte)1, resultSet.getByte(3));

		assertEquals((byte)1, resultSet.getObject(4, Byte.class));
		assertEquals((byte)1, resultSet.getByte(4));

		assertTransformationError(5, Byte.class);
		assertTransformationError(5, i -> resultSet.getByte(i));

		assertTransformationError(6, Byte.class);
		assertTransformationError(6, i -> resultSet.getByte(i));
	}

//	@Test
	void shouldGetShort() throws SQLException {
		inputStream = getInputStreamWithNumericTypes();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		assertEquals((short)1, resultSet.getObject(1, Short.class));
		assertEquals((short)1, resultSet.getShort(1));

		assertTransformationError(2, Short.class);
		assertTransformationError(2, i -> resultSet.getShort(i));

		assertEquals((short)1, resultSet.getObject(3, Short.class));
		assertEquals((short)1, resultSet.getShort(3));

		assertEquals((short)1, resultSet.getObject(4, Short.class));
		assertEquals((short)1, resultSet.getShort(4));

		assertTransformationError(5, Short.class);
		assertTransformationError(5, i -> resultSet.getShort(i));

		assertEquals((short)30000, resultSet.getObject(6, Short.class));
		assertEquals((short)30000, resultSet.getShort(6));
	}

//	@Test
	void shouldGetInt() throws SQLException {
		inputStream = getInputStreamWithNumericTypes();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		assertEquals(1, resultSet.getObject(1, Integer.class));
		assertEquals(1, resultSet.getInt(1));

		assertTransformationError(2, Integer.class);
		assertTransformationError(2, i -> resultSet.getInt(i));

		assertEquals(1, resultSet.getObject(3, Integer.class));
		assertEquals(1, resultSet.getInt(3));

		assertEquals(1, resultSet.getObject(4, Integer.class));
		assertEquals(1, resultSet.getInt(4));

		assertEquals(1231232, resultSet.getObject(5, Integer.class));
		assertEquals(1231232, resultSet.getInt(5));

		assertEquals(30000, resultSet.getObject(6, Integer.class));
		assertEquals(30000, resultSet.getInt(6));
	}

//	@Test
	void shouldGetLong() throws SQLException {
		inputStream = getInputStreamWithNumericTypes();
		resultSet = createResultSet(inputStream);
		resultSet.next();

		assertEquals(1L, resultSet.getObject(1, Long.class));
		assertEquals(1L, resultSet.getLong(1));

		assertEquals(30000000000L, resultSet.getObject(2, Long.class));
		assertEquals(30000000000L, resultSet.getLong(2));

		assertEquals(1, resultSet.getObject(3, Long.class));
		assertEquals(1, resultSet.getLong(3));

		assertEquals(1, resultSet.getObject(4, Long.class));
		assertEquals(1, resultSet.getLong(4));

		assertEquals(1231232L, resultSet.getObject(5, Long.class));
		assertEquals(1231232L, resultSet.getLong(5));

		assertEquals(30000L, resultSet.getObject(6, Long.class));
		assertEquals(30000L, resultSet.getLong(6));
	}

//	@Test
	void shouldGetBigInteger() throws SQLException {
		inputStream = getInputStreamWithNumericTypes();
		resultSet = createResultSet(inputStream);
		resultSet.next();

		assertEquals(new BigInteger("1"), resultSet.getObject(1, BigInteger.class));
		assertEquals(new BigInteger("30000000000"), resultSet.getObject(2, BigInteger.class));
		assertEquals(new BigInteger("1"), resultSet.getObject(3, BigInteger.class));
		assertEquals(new BigInteger("1"), resultSet.getObject(4, BigInteger.class));
		assertEquals(new BigInteger("1231232"), resultSet.getObject(5, BigInteger.class));
		assertEquals(new BigInteger("30000"), resultSet.getObject(6, BigInteger.class));
	}

//	@Test
	void shouldGetFloat() throws SQLException {
		inputStream = getInputStreamWithNumericTypes();
		resultSet = createResultSet(inputStream);
		resultSet.next();

		assertEquals(1, resultSet.getObject(1, Float.class));
		assertEquals(1.F, resultSet.getFloat(1));

		assertEquals(30000000000f, resultSet.getObject(2, Float.class));
		assertEquals(30000000000.F, resultSet.getFloat(2));

		assertEquals(1.23f, resultSet.getObject(3, Float.class));
		assertEquals(1.23f, resultSet.getFloat(3));

		assertEquals(1.23456789012f, resultSet.getObject(4, Float.class));
		assertEquals(1.23456789012f, resultSet.getFloat(4));

		assertEquals(1231232.123459999990457054844258706536f, resultSet.getObject(5, Float.class), 0.01);
		assertEquals(1231232.123459999990457054844258706536f, resultSet.getFloat(5), 0.01);

		assertEquals(30000.F, resultSet.getObject(6, Float.class));
		assertEquals(30000.F, resultSet.getFloat(6));
	}

//	@Test
	void shouldGetDouble() throws SQLException {
		inputStream = getInputStreamWithNumericTypes();
		resultSet = createResultSet(inputStream);
		resultSet.next();

		assertEquals(1, resultSet.getObject(1, Double.class));
		assertEquals(1., resultSet.getDouble(1));

		assertEquals(30000000000., resultSet.getObject(2, Double.class));
		assertEquals(30000000000., resultSet.getDouble(2));

		assertEquals(1.23, resultSet.getObject(3, Double.class));
		assertEquals(1.23, resultSet.getDouble(3));

		assertEquals(1.23456789012, resultSet.getObject(4, Double.class));
		assertEquals(1.23456789012, (double)resultSet.getObject(4, Map.of("double precision", Double.class)), 0.01);
		assertEquals(new BigDecimal("1.23456789012"), resultSet.getBigDecimal(4));

		assertEquals(1231232.123459999990457054844258706536, resultSet.getObject(5, Double.class), 0.01);
		assertEquals(1231232.123459999990457054844258706536, resultSet.getDouble(5), 0.01);

		assertEquals(30000., resultSet.getObject(6, Double.class));
		assertEquals(30000., resultSet.getDouble(6));
	}

//	@Test
	void shouldGetBigDecimal() throws SQLException {
		inputStream = getInputStreamWithNumericTypes();
		resultSet = createResultSet(inputStream);
		resultSet.next();

		assertEquals(new BigDecimal("1"), resultSet.getObject(1, BigDecimal.class));
		assertEquals(new BigDecimal("1"), resultSet.getBigDecimal(1));

		assertEquals(new BigDecimal("30000000000"), resultSet.getObject(2, BigDecimal.class));
		assertEquals(new BigDecimal("30000000000"), resultSet.getBigDecimal(2));

		assertEquals(new BigDecimal("1.23"), resultSet.getObject(3, BigDecimal.class));
		assertEquals(new BigDecimal("1.23"), resultSet.getBigDecimal(3));

		assertEquals(new BigDecimal("1.23456789012"), resultSet.getObject(4, BigDecimal.class));
		assertEquals(new BigDecimal("1.23456789012"), resultSet.getBigDecimal(4));

		assertEquals(new BigDecimal("1231232.123459999990457054844258706536"), resultSet.getObject(5, BigDecimal.class));
		assertEquals(new BigDecimal("1231232.123459999990457054844258706536"), resultSet.getBigDecimal(5));

		assertEquals(new BigDecimal("30000"), resultSet.getObject(6, BigDecimal.class));
		assertEquals(new BigDecimal("30000"), resultSet.getBigDecimal(6));
	}

	private <T> void assertTransformationError(int columnIndex, Class<T> type) {
		assertTransformationError(columnIndex, i -> resultSet.getObject(i, type));
	}

	private <T> void assertTransformationError(int columnIndex, CheckedFunction<Integer, T> getter) {
		FireboltException e = assertThrows(FireboltException.class, () -> getter.apply(columnIndex));
		assertEquals(TYPE_TRANSFORMATION_ERROR, e.getType());
		assertEquals(NumberFormatException.class, e.getCause().getClass());
	}

//	@Test
	void shouldReturnDataWithProvidedTypes() throws SQLException {
		inputStream = getInputStreamWithNewTypes();
		resultSet = createResultSet(inputStream);
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

//	@Test
	void shouldReturnDataAndTypesForNumericTypes() throws SQLException {
		inputStream = getInputStreamWithNumericTypes();
		resultSet = createResultSet(inputStream);
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
		assertEquals(30000, resultSet.getObject(6));
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

//	@Test
	void shouldReturnDateTimeObjectsWithProvidedTypes() throws SQLException {
		inputStream = getInputStreamWithNewTypes();
		resultSet = createResultSet(inputStream);
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

//	@Test
	void shouldThrowExceptionWhenConvertingIncompatibleTypes() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		FireboltException exception = assertThrows(FireboltException.class, () -> resultSet.getObject(1, String.class));
		assertEquals("conversion to class java.lang.String from java.lang.Integer not supported", exception.getMessage());
	}

//	@Test
	void shouldThrowExceptionWhenConvertingUnsupportedTypes() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		FireboltException exception = assertThrows(FireboltException.class, () -> resultSet.getObject(1, TimeZone.class));
		assertEquals("conversion to java.util.TimeZone from java.lang.Integer not supported", exception.getMessage());
	}

//	@Test
	void shouldConvertInt64s() throws SQLException {
		inputStream = getInputStreamWithBigInt64();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		assertEquals(new BigInteger("18446744073709551615"), resultSet.getObject(1, BigInteger.class));
		assertEquals(new BigInteger("-9223372036854775807"), resultSet.getObject(2, BigInteger.class));
	}

//	@Test
	void shouldThrowIntegerInfinity() throws SQLException {
		inputStream = getInputStreamWithInfinity();
		resultSet = createResultSet(inputStream);
		resultSet.next();

		assertIllegalArgumentExceptionCause(() -> resultSet.getShort(1));
		assertIllegalArgumentExceptionCause(() -> resultSet.getInt(1));
		assertIllegalArgumentExceptionCause(() -> resultSet.getLong(1));

		assertIllegalArgumentExceptionCause(() -> resultSet.getShort(2));
		assertIllegalArgumentExceptionCause(() -> resultSet.getInt(2));
		assertIllegalArgumentExceptionCause(() -> resultSet.getLong(2));

		assertIllegalArgumentExceptionCause(() -> resultSet.getObject(1, BigInteger.class));
	}

	private void assertIllegalArgumentExceptionCause(Executable getter) {
		assertEquals(IllegalArgumentException.class, assertThrows(SQLException.class, getter).getCause().getClass());
	}

//	@Test
	void shouldConvertFloatInfinity() throws SQLException {
		inputStream = getInputStreamWithInfinity();
		resultSet = createResultSet(inputStream);
		resultSet.next();

		assertEquals(Float.POSITIVE_INFINITY, resultSet.getFloat(1));
		assertEquals(Float.NEGATIVE_INFINITY, resultSet.getFloat(2));

		assertEquals(Float.POSITIVE_INFINITY, resultSet.getDouble(1));
		assertEquals(Float.NEGATIVE_INFINITY, resultSet.getDouble(2));

		assertEquals(Double.POSITIVE_INFINITY, resultSet.getObject(1, Double.class));
		assertEquals(Double.NEGATIVE_INFINITY, resultSet.getObject(2, Double.class));
		assertEquals(Float.POSITIVE_INFINITY, resultSet.getObject(1, Float.class));
		assertEquals(Float.NEGATIVE_INFINITY, resultSet.getObject(2, Float.class));
	}

//	@Test
	void shouldConvertFloatNotANumber() throws SQLException {
		inputStream = getInputStreamWithInfinity();
		resultSet = createResultSet(inputStream);
		resultSet.next();

		assertEquals(Float.NaN, resultSet.getFloat(3));
		assertEquals(Double.NaN, resultSet.getDouble(3));
		assertEquals(Float.NaN, resultSet.getFloat(4));
		assertEquals(Double.NaN, resultSet.getDouble(4));
	}

//	@Test
	void shouldReadArray() throws SQLException {
		inputStream = getInputStreamWithArray();
		resultSet = createResultSet(inputStream);
		assertTrue(resultSet.next());
		Object intArray = resultSet.getObject(1);
		assertEquals(Integer[].class, intArray.getClass());
		assertArrayEquals(new Integer[]{1, 2, 3}, (Integer[]) intArray);
		assertFalse(resultSet.next());
	}

//	@Test
	void unwrap() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		for (Class<?> type : new Class[] {ResultSet.class, Wrapper.class, AutoCloseable.class, FireboltResultSet.class}) {
			assertTrue(resultSet.isWrapperFor(type));
			assertSame(resultSet, resultSet.unwrap(type));
		}
		assertFalse(resultSet.isWrapperFor(Runnable.class));
		assertEquals("Cannot unwrap to " + Runnable.class.getName(), assertThrows(SQLException.class, () -> resultSet.unwrap(Runnable.class)).getMessage());
	}

//	@Test
	@SuppressWarnings("java:S1874") // getUnicodeStream is deprecated byt must be tested
	void shouldReturnStream() throws SQLException, IOException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		String expectedStr = "Taylor's Prime Steak House";
		byte[] expected = expectedStr.getBytes();
		assertStream(expected, resultSet.getAsciiStream(3));
		assertStream(expected, resultSet.getAsciiStream("name"));
		//noinspection deprecation
		assertStream(expected, resultSet.getUnicodeStream(3));
		//noinspection deprecation
		assertStream(expected, resultSet.getUnicodeStream("name"));
		assertEquals(expectedStr, IOUtils.toString(resultSet.getCharacterStream(3)));
		assertEquals(expectedStr, IOUtils.toString(resultSet.getCharacterStream("name")));
		assertEquals(expectedStr, IOUtils.toString(resultSet.getNCharacterStream(3)));
		assertEquals(expectedStr, IOUtils.toString(resultSet.getNCharacterStream("name")));
	}

//	@Test
	void shouldReturnUrl() throws SQLException, MalformedURLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		assertEquals(new URL("http://firebolt.io"), resultSet.getURL(8));
		assertEquals(new URL("http://firebolt.io"), resultSet.getURL("url"));
		assertEquals(MalformedURLException.class, assertThrows(SQLException.class, () -> resultSet.getURL(3)).getCause().getClass());
		assertEquals(MalformedURLException.class, assertThrows(SQLException.class, () -> resultSet.getURL("name")).getCause().getClass());
		resultSet.next();
		assertNull(resultSet.getURL(8));
		assertNull(resultSet.getURL("url"));
	}

//	@Test
	void shouldReturnGeography() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		String expectedValue = "0101000020E6100000FEFFFFFFFFFFEF3F000000000000F03F";
		assertEquals(expectedValue, resultSet.getObject(9));
		assertEquals(expectedValue, resultSet.getObject("location"));
		assertEquals(expectedValue, resultSet.getString(9));
		assertEquals(expectedValue, resultSet.getString("location"));
		// Returns native JDBC type
		assertEquals(Types.VARCHAR, resultSet.getMetaData().getColumnType(9));
	}

//	@Test
	void shouldReturnStruct() throws SQLException {
		inputStream = getInputStreamWithStruct();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		assertEquals("{\"a\":null}", resultSet.getObject(2));
		assertEquals("{\"a\":null}", resultSet.getObject("null_struct"));
		assertEquals("{\"a\":\"1\"}", resultSet.getObject(4));
		assertEquals("{\"a\":\"1\"}", resultSet.getObject("a_struct"));
		assertEquals("{\"a\":[1,2,3]}", resultSet.getObject(5));
		assertEquals("{\"a\":[1,2,3]}", resultSet.getObject("array_struct"));
		assertEquals("{\"x\":\"2\",\"a\":{\"b col\":\"1\",\"c\":\"3\"}}", resultSet.getObject(6));
		assertEquals("{\"x\":\"2\",\"a\":{\"b col\":\"1\",\"c\":\"3\"}}", resultSet.getObject("nested_struct"));
		// Returns native JDBC type
		for (int i = 2; i <= 6; i++) {
			assertEquals(Types.VARCHAR, resultSet.getMetaData().getColumnType(i));
		}

		assertEquals("STRUCT(A INT NULL)", resultSet.getMetaData().getColumnTypeName(2));
		assertEquals("STRUCT(A INT)", resultSet.getMetaData().getColumnTypeName(3));
		assertEquals("STRUCT(A INT)", resultSet.getMetaData().getColumnTypeName(4));
		assertEquals("STRUCT(A ARRAY(INT))", resultSet.getMetaData().getColumnTypeName(5));
		assertEquals("STRUCT(X INT, A STRUCT(`B COL` INT, C INT))", resultSet.getMetaData().getColumnTypeName(6));
	}

//	@Test
	void shouldBeCaseInsensitive() throws SQLException {
		inputStream = getInputStreamWithCommonResponseExample();
		resultSet = createResultSet(inputStream);
		resultSet.next();
		ResultSetMetaData rsmd = resultSet.getMetaData();
		int n = rsmd.getColumnCount();
		for (int i = 1; i <= n; i++) {
			String columnName = rsmd.getColumnName(i);
			Object value = resultSet.getObject(columnName);
			Object upperCaseValue = resultSet.getObject(columnName.toUpperCase());
			Object lowerCaseValue = resultSet.getObject(columnName.toLowerCase());
			if (rsmd.getColumnType(i) == Types.ARRAY) {
				assertArrayEquals((Object[])value, (Object[])upperCaseValue);
				assertArrayEquals((Object[])value, (Object[])lowerCaseValue);
			} else {
				assertEquals(value, upperCaseValue);
				assertEquals(value, lowerCaseValue);
			}
		}
	}

	private void assertStream(byte[] expected, InputStream stream) throws IOException {
		assertArrayEquals(expected, stream == null ? null : stream.readAllBytes());
	}

	private InputStream getInputStreamWithCommonResponseExample() {
//		return FireboltResultSetTest.class.getResourceAsStream("/responses/firebolt-response-example");
		return FireboltResultSetTest.class.getResourceAsStream("/responses/firebolt-response-example-json-lines");
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

	private InputStream getInputStreamWithInfinity() {
		return FireboltResultSetTest.class.getResourceAsStream("/responses/firebolt-response-with-infinity");
	}

	private InputStream getInputStreamWithNewTypes() {
		return FireboltResultSetTest.class.getResourceAsStream("/responses/firebolt-response-with-new-types");
	}

	private InputStream getInputStreamWithNumericTypes() {
		return FireboltResultSetTest.class.getResourceAsStream("/responses/firebolt-response-with-numeric-types.csv");
	}

	private InputStream getInputStreamWithArray() {
		return FireboltResultSetTest.class.getResourceAsStream("/responses/firebolt-response-with-array");
	}

	private InputStream getInputStreamWithStruct() {
		return FireboltResultSetTest.class.getResourceAsStream("/responses/firebolt-response-with-struct-nofalse");
	}

	private ResultSet createResultSet(InputStream is) throws SQLException {
		return new FireboltResultSet(is, "a_table", "a_db", 65535, false, fireboltStatement, true);
	}
}
