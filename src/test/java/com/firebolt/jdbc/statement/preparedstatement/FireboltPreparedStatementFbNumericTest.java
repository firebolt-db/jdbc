package com.firebolt.jdbc.statement.preparedstatement;

import com.firebolt.jdbc.client.query.StatementClient;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.service.FireboltStatementService;
import com.firebolt.jdbc.statement.StatementInfoWrapper;
import com.firebolt.jdbc.type.lob.FireboltClob;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junitpioneer.jupiter.DefaultTimeZone;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Optional;
import java.util.TimeZone;
import java.util.stream.Stream;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FireboltPreparedStatementFbNumericTest {

	@Captor
	ArgumentCaptor<StatementInfoWrapper> queryInfoWrapperArgumentCaptor;
	@Mock
	private FireboltStatementService fireboltStatementService;
	@Mock
	private FireboltProperties properties;

	private final FireboltConnection connection = Mockito.mock(FireboltConnection.class);
	private static FireboltPreparedStatement statement;

	private interface Setter {
		void set(PreparedStatement ps) throws SQLException;
	}

	private interface ParameterizedSetter<T> {
		void set(PreparedStatement statement, int index, T value) throws SQLException;
	}

	private static Stream<Arguments> unsupported() {
		return Stream.of(
					Arguments.of("setRef", (Executable) () -> statement.setRef(1, mock(Ref.class))),
					Arguments.of("setTime", (Executable) () -> statement.setTime(1, new Time(System.currentTimeMillis()))),
					Arguments.of("setTime(calendar)", (Executable) () -> statement.setTime(1, new Time(System.currentTimeMillis()), Calendar.getInstance())),
					Arguments.of("setRowId", (Executable) () -> statement.setRowId(1, mock(RowId.class))),
					Arguments.of("setSQLXML", (Executable) () -> statement.setSQLXML(1, mock(SQLXML.class))),
					Arguments.of("getParameterMetaData", (Executable) () -> statement.getParameterMetaData())
		);
	}

	private static Stream<Arguments> buffers() {
		return Stream.of(
				Arguments.of("setClob((Clob)null)", (Setter) statement -> statement.setClob(1, (Clob)null), "null"),
				Arguments.of("setClob((Reader)null)", (Setter) statement -> statement.setClob(1, (Reader)null), "null"),
				Arguments.of("setClob((Reader)null, length)", (Setter) statement -> statement.setClob(1, null, 1L), "null"),
				Arguments.of("setClob(Reader)", (Setter) statement -> statement.setClob(1, new FireboltClob("hello".toCharArray())), "\"hello\""),
				Arguments.of("setClob(Reader, length=)", (Setter) statement -> statement.setClob(1, new StringReader("hello"), 5), "\"hello\""),
				Arguments.of("setClob(Reader, length-1)", (Setter) statement -> statement.setClob(1, new StringReader("hello"), 4), "\"hell\""),
				Arguments.of("setClob(Reader, length+1)", (Setter) statement -> statement.setClob(1, new StringReader("hello"), 6), "\"hello\""),
				Arguments.of("setClob(Reader, 42)", (Setter) statement -> statement.setClob(1, new StringReader("hello"), 42), "\"hello\""),
				Arguments.of("setClob(Reader, 1)", (Setter) statement -> statement.setClob(1, new StringReader("hello"), 1), "\"h\""),
				Arguments.of("setClob(Reader, 0)", (Setter) statement -> statement.setClob(1, new StringReader("hello"), 0), "\"\""),

				Arguments.of("setNClob((NClob)null)", (Setter) statement -> statement.setNClob(1, (NClob)null), "null"),
				Arguments.of("setNClob((Reader)null)", (Setter) statement -> statement.setNClob(1, (Reader)null), "null"),
				Arguments.of("setNClob((Reader)null, length)", (Setter) statement -> statement.setNClob(1, null, 1L), "null"),
				Arguments.of("setClob(Reader)", (Setter) statement -> statement.setNClob(1, new FireboltClob("hello".toCharArray())), "\"hello\""),
				Arguments.of("setNClob(Reader, length=)", (Setter) statement -> statement.setNClob(1, new StringReader("hello"), 5), "\"hello\""),
				Arguments.of("setNClob(Reader, length-1)", (Setter) statement -> statement.setNClob(1, new StringReader("hello"), 4), "\"hell\""),
				Arguments.of("setNClob(Reader, length+1)", (Setter) statement -> statement.setNClob(1, new StringReader("hello"), 6), "\"hello\""),
				Arguments.of("setNClob(Reader, 42)", (Setter) statement -> statement.setNClob(1, new StringReader("hello"), 42), "\"hello\""),
				Arguments.of("setNClob(Reader, 1)", (Setter) statement -> statement.setNClob(1, new StringReader("hello"), 1), "\"h\""),
				Arguments.of("setNClob(Reader, 0)", (Setter) statement -> statement.setNClob(1, new StringReader("hello"), 0), "\"\""),

				Arguments.of("setBlob((Blob)null)", (Setter) statement -> statement.setBlob(1, (Blob)null), "null"),
				Arguments.of("setBClob((InputStream)null)", (Setter) statement -> statement.setBlob(1, (InputStream)null), "null"),
				Arguments.of("setBClob((InputStream)null, length)", (Setter) statement -> statement.setBlob(1, null, 1L), "null"),

				Arguments.of("setCharacterStream(null)", (Setter) statement -> statement.setCharacterStream(1, null), "null"),
				Arguments.of("setCharacterStream(null, int)", (Setter) statement -> statement.setCharacterStream(1, null, 1), "null"),
				Arguments.of("setCharacterStream(null, long)", (Setter) statement -> statement.setCharacterStream(1, null, 1L), "null"),
				Arguments.of("setCharacterStream(Reader)", (Setter) statement -> statement.setCharacterStream(1, new StringReader("hello")), "\"hello\""),
				Arguments.of("setCharacterStream(Reader, length=)", (Setter) statement -> statement.setCharacterStream(1, new StringReader("hello"), 5), "\"hello\""),
				Arguments.of("setCharacterStream(Reader, length-1)", (Setter) statement -> statement.setCharacterStream(1, new StringReader("hello"), 4), "\"hell\""),
				Arguments.of("setCharacterStream(Reader, length+1)", (Setter) statement -> statement.setCharacterStream(1, new StringReader("hello"), 6), "\"hello\""),
				Arguments.of("setCharacterStream(Reader, 42)", (Setter) statement -> statement.setCharacterStream(1, new StringReader("hello"), 42), "\"hello\""),
				Arguments.of("setCharacterStream(Reader, 1)", (Setter) statement -> statement.setCharacterStream(1, new StringReader("hello"), 1), "\"h\""),
				Arguments.of("setCharacterStream(Reader, 0)", (Setter) statement -> statement.setCharacterStream(1, new StringReader("hello"), 0), "\"\""),

				Arguments.of("setNCharacterStream(null)", (Setter) statement -> statement.setNCharacterStream(1, null), "null"),
				Arguments.of("setNCharacterStream(null, int)", (Setter) statement -> statement.setNCharacterStream(1, null, 1), "null"),
				Arguments.of("setNCharacterStream(null, long)", (Setter) statement -> statement.setNCharacterStream(1, null, 1L), "null"),
				Arguments.of("setNCharacterStream(Reader)", (Setter) statement -> statement.setNCharacterStream(1, new StringReader("hello")), "\"hello\""),
				Arguments.of("setNCharacterStream(Reader, length=)", (Setter) statement -> statement.setNCharacterStream(1, new StringReader("hello"), 5), "\"hello\""),
				Arguments.of("setNCharacterStream(Reader, length-1)", (Setter) statement -> statement.setNCharacterStream(1, new StringReader("hello"), 4), "\"hell\""),
				Arguments.of("setNCharacterStream(Reader, length+1)", (Setter) statement -> statement.setNCharacterStream(1, new StringReader("hello"), 6), "\"hello\""),
				Arguments.of("setNCharacterStream(Reader, 42)", (Setter) statement -> statement.setNCharacterStream(1, new StringReader("hello"), 42), "\"hello\""),
				Arguments.of("setNCharacterStream(Reader, 1)", (Setter) statement -> statement.setNCharacterStream(1, new StringReader("hello"), 1), "\"h\""),
				Arguments.of("setNCharacterStream(Reader, 0)", (Setter) statement -> statement.setNCharacterStream(1, new StringReader("hello"), 0), "\"\""),

				Arguments.of("setAsciiStream(null)", (Setter) statement -> statement.setAsciiStream(1, null), "null"),
				Arguments.of("setAsciiStream(null, int)", (Setter) statement -> statement.setAsciiStream(1, null, 1), "null"),
				Arguments.of("setAsciiStream(null, long)", (Setter) statement -> statement.setAsciiStream(1, null, 1L), "null"),

				Arguments.of("setBinaryStream(null)", (Setter) statement -> statement.setBinaryStream(1, null), "null"),
				Arguments.of("setBinaryStream(null, int)", (Setter) statement -> statement.setBinaryStream(1, null, 1), "null"),
				Arguments.of("setBinaryStream(null, long)", (Setter) statement -> statement.setBinaryStream(1, null, 1L), "null"));
	}

	private static Stream<Arguments> setNumber() {
		return Stream.of(
				Arguments.of("byte", (ParameterizedSetter<Byte>) PreparedStatement::setByte, (byte)50),
				Arguments.of("short", (ParameterizedSetter<Short>) PreparedStatement::setShort, (short)50),
				Arguments.of("int", (ParameterizedSetter<Integer>) PreparedStatement::setInt, 50),
				Arguments.of("long", (ParameterizedSetter<Long>) PreparedStatement::setLong, 50L),
				Arguments.of("float", (ParameterizedSetter<Float>) PreparedStatement::setFloat, 5.5f),
				Arguments.of("double", (ParameterizedSetter<Double>) PreparedStatement::setDouble, 3.14),
				Arguments.of("double", (ParameterizedSetter<BigDecimal>) PreparedStatement::setBigDecimal, new BigDecimal("555555555555.55555555"))
		);
	}

	@BeforeEach
	void beforeEach() throws SQLException {
		when(connection.getSessionProperties()).thenReturn(properties);
		lenient().when(properties.getPreparedStatementParamStyle()).thenReturn("fb_numeric");
		lenient().when(properties.getBufferSize()).thenReturn(10);
		lenient().when(fireboltStatementService.execute(any(), any(), any())).thenReturn(Optional.empty());
	}

	@AfterEach
	void afterEach() throws SQLException {
		if (statement != null) {
			statement.close();
		}
	}

	@ParameterizedTest
	@CsvSource(value = {
			"INSERT INTO data (field) VALUES ($1),false",
			"SELECT * FROM data WHERE field=$1,true",
	})
	void getMetadata(String query, boolean expectedResultSet) throws SQLException {
		StatementClient statementClient = mock(StatementClient.class);
		when(statementClient.executeSqlStatement(any(), any(), anyBoolean(), anyInt(), anyBoolean())).thenReturn(new ByteArrayInputStream(new byte[0]));
		statement = new FireboltPreparedStatement(new FireboltStatementService(statementClient), connection, query);
		assertNull(statement.getMetaData());
		statement.setObject(1, null);
		boolean shouldHaveResultSet = statement.execute();
		assertEquals(expectedResultSet, shouldHaveResultSet);
		if (shouldHaveResultSet) {
			assertNotNull(statement.getMetaData());
		} else {
			assertNull(statement.getMetaData());
		}
	}

	@Test
	void shouldExecute() throws SQLException {
		statement = createStatementWithSql("INSERT INTO cars (sales, make, model, minor_model, color, type) VALUES ($1,$2,$3,$4,$5,$6)");

		statement.setInt(1, 500);
		statement.setString(2, "Ford");
		statement.setObject(3, "FOCUS", Types.VARCHAR);
		statement.setNull(4, Types.VARCHAR);
		statement.setNull(5, Types.VARCHAR,  "VARCHAR");
		statement.setNString(6, "sedan");
		statement.execute();
		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), any());
		assertEquals("INSERT INTO cars (sales, make, model, minor_model, color, type) VALUES ($1,$2,$3,$4,$5,$6)",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
		assertEquals("[{\"name\":\"$1\",\"value\":500},{\"name\":\"$2\",\"value\":\"Ford\"}," +
				"{\"name\":\"$3\",\"value\":\"FOCUS\"},{\"name\":\"$4\",\"value\":null},{\"name\":\"$5\",\"value\":null}," +
				"{\"name\":\"$6\",\"value\":\"sedan\"}]",
				queryInfoWrapperArgumentCaptor.getValue().getPreparedStatementParameters());
	}

	@Test
	void setNullByteArray() throws SQLException {
		statement = createStatementWithSql("INSERT INTO cars (sales, make, model, minor_model, color, type, signature) VALUES ($1,$2,$3,$4,$5,$6,$7)");

		statement.setShort(1, (short)500);
		statement.setString(2, "Ford");
		statement.setObject(3, "FOCUS", Types.VARCHAR);
		statement.setNull(4, Types.VARCHAR);
		statement.setNull(5, Types.VARCHAR,  "VARCHAR");
		statement.setNString(6, "sedan");
		statement.setBytes(7, null);
		statement.execute();
		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), any());
		assertEquals("INSERT INTO cars (sales, make, model, minor_model, color, type, signature) VALUES ($1,$2,$3,$4,$5,$6,$7)",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
		assertEquals("[{\"name\":\"$1\",\"value\":500},{\"name\":\"$2\",\"value\":\"Ford\"}," +
						"{\"name\":\"$3\",\"value\":\"FOCUS\"},{\"name\":\"$4\",\"value\":null},{\"name\":\"$5\",\"value\":null}," +
						"{\"name\":\"$6\",\"value\":\"sedan\"},{\"name\":\"$7\",\"value\":null}]",
				queryInfoWrapperArgumentCaptor.getValue().getPreparedStatementParameters());
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("buffers")
	void setBuffer(String name, Setter setter, String expected) throws SQLException {
		statement = createStatementWithSql("INSERT INTO data (field) VALUES ($1)");
		setter.set(statement);
		statement.execute();
		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), any());
		assertEquals("INSERT INTO data (field) VALUES ($1)", queryInfoWrapperArgumentCaptor.getValue().getSql());
		assertEquals("[{\"name\":\"$1\",\"value\":" + expected + "}]",
				queryInfoWrapperArgumentCaptor.getValue().getPreparedStatementParameters());
	}

	@Test
	void setFailingCharacterStream() throws IOException {
		statement = createStatementWithSql("INSERT INTO data (field) VALUES (?)");
		Reader reader = mock(Reader.class);
		when(reader.read(any(), anyInt(), anyInt())).thenThrow(new IOException());
		assertEquals(IOException.class, assertThrows(SQLException.class, () -> statement.setCharacterStream(1, reader)).getCause().getClass());
	}

	@Test
	void setFailingBinaryStream() throws IOException {
		statement = createStatementWithSql("INSERT INTO data (field) VALUES (?)");
		InputStream is = mock(InputStream.class);
		when(is.readAllBytes()).thenThrow(new IOException());
		assertEquals(IOException.class, assertThrows(SQLException.class, () -> statement.setBinaryStream(1, is)).getCause().getClass());
	}

	@Test
	void setFailingLimitedBinaryStream() throws IOException {
		statement = createStatementWithSql("INSERT INTO data (field) VALUES (?)");
		InputStream is = mock(InputStream.class);
		when(is.readNBytes(1024)).thenThrow(new IOException());
		assertEquals(IOException.class, assertThrows(SQLException.class, () -> statement.setBinaryStream(1, is, 1024)).getCause().getClass());
	}

	@Test
	void shouldExecuteBatch() throws SQLException {
		statement = createStatementWithSql("INSERT INTO cars (sales, make) VALUES ($1,$2)");

		statement.setObject(1, 150);
		statement.setObject(2, "Ford");
		statement.addBatch();
		statement.setObject(1, 300);
		statement.setObject(2, "Tesla");
		statement.addBatch();
		statement.executeBatch();
		verify(fireboltStatementService, times(2)).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), any());
		assertEquals("INSERT INTO cars (sales, make) VALUES ($1,$2)",
				queryInfoWrapperArgumentCaptor.getAllValues().get(0).getSql());
		assertEquals("INSERT INTO cars (sales, make) VALUES ($1,$2)",
				queryInfoWrapperArgumentCaptor.getAllValues().get(1).getSql());
		assertEquals("[{\"name\":\"$1\",\"value\":150},{\"name\":\"$2\",\"value\":\"Ford\"}]",
				queryInfoWrapperArgumentCaptor.getAllValues().get(0).getPreparedStatementParameters());
		assertEquals("[{\"name\":\"$1\",\"value\":300},{\"name\":\"$2\",\"value\":\"Tesla\"}]",
				queryInfoWrapperArgumentCaptor.getAllValues().get(1).getPreparedStatementParameters());
	}

	@Test
	void shouldThrowExceptionWhenAllParametersAreNotDefined() throws SQLException {
		try (PreparedStatement ps = createStatementWithSql("SELECT * FROM cars WHERE make LIKE $1")) {
			when(fireboltStatementService.execute(any(), any(), any()))
					.thenThrow(
						new FireboltException(
							"Line 1, Column 8: Query referenced positional parameter $3, but it was not set",
							400,
							"Line 1, Column 8: Query referenced positional parameter $3, but it was not set"
						)
					);
			assertEquals("Line 1, Column 8: Query referenced positional parameter $3, but it was not set",
					assertThrows(FireboltException.class, ps::execute).getErrorMessageFromServer());
		}
	}

	@Test
	void shouldExecuteWithSpecialCharactersInQuery() throws SQLException {
		String sql = "INSERT INTO cars (model ,sales, make) VALUES ($1,$2,'($3:^|[^\\\\p{L}\\\\p{N}])($4i)(phone)($5:[^\\\\p{L}\\\\p{N}]|$)')";
		statement = createStatementWithSql(sql);

		statement.setObject(1, "?");
		statement.setObject(2, " ?");

		statement.execute();
		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), any());
		assertEquals(sql, queryInfoWrapperArgumentCaptor.getValue().getSql());
		assertEquals("[{\"name\":\"$1\",\"value\":\"?\"},{\"name\":\"$2\",\"value\":\" ?\"}]",
				queryInfoWrapperArgumentCaptor.getValue().getPreparedStatementParameters());
	}

	@Test
	void shouldExecuteUpdate() throws SQLException {
		statement  = createStatementWithSql("update cars set sales = $1 where make = $2");

		statement.setObject(1, 150);
		statement.setObject(2, "Ford");

		assertEquals(0, statement.executeUpdate()); // we are not able to return number of affected lines right now

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), any());
		assertEquals("update cars set sales = $1 where make = $2",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
		assertEquals("[{\"name\":\"$1\",\"value\":150},{\"name\":\"$2\",\"value\":\"Ford\"}]",
				queryInfoWrapperArgumentCaptor.getValue().getPreparedStatementParameters());
	}


	@Test
	void shouldThrowsExceptionWhenTryingToExecuteUpdateWithQuery() throws SQLException {
		statement  = createStatementWithSql("update cars set sales = $1 where make = $2");

		statement.setObject(1, 150);
		statement.setObject(2, "Ford");

		assertThrows(FireboltException.class,
				() -> statement.executeUpdate("update cars set sales = 50 where make = 'Ford"));
	}

	@Test
	void shouldSetNull() throws SQLException {
		statement = createStatementWithSql("INSERT INTO cars (sales, make) VALUES ($1,$2)");

		statement.setNull(1, 0);
		statement.setNull(2, 0);
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), any());
		assertEquals("INSERT INTO cars (sales, make) VALUES ($1,$2)",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
		assertEquals("[{\"name\":\"$1\",\"value\":null},{\"name\":\"$2\",\"value\":null}]",
				queryInfoWrapperArgumentCaptor.getValue().getPreparedStatementParameters());
	}

	@Test
	void shouldSetBoolean() throws SQLException {
		statement = createStatementWithSql("INSERT INTO cars(available) VALUES ($1)");

		statement.setBoolean(1, true);
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), any());
		assertEquals("INSERT INTO cars(available) VALUES ($1)", queryInfoWrapperArgumentCaptor.getValue().getSql());
		assertEquals("[{\"name\":\"$1\",\"value\":true}]", queryInfoWrapperArgumentCaptor.getValue().getPreparedStatementParameters());
	}

	@ParameterizedTest
	@CsvSource(value = {
			"Nobody,",
			"Firebolt,http://www.firebolt.io"
	})
	void shouldSetUrl(String name, URL url) throws SQLException {
		statement = createStatementWithSql("INSERT INTO companies (name, url) VALUES ($1,$2)");

		statement.setString(1, name);
		statement.setURL(2, url);
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), any());
		assertEquals("INSERT INTO companies (name, url) VALUES ($1,$2)", queryInfoWrapperArgumentCaptor.getValue().getSql());
		assertEquals("[{\"name\":\"$1\",\"value\":\"" + name + "\"}," +
				"{\"name\":\"$2\",\"value\":" + sqlQuote(url) + "}]",
				queryInfoWrapperArgumentCaptor.getValue().getPreparedStatementParameters());
	}

	private String sqlQuote(Object value) {
		return value == null ? "null" : format("\"%s\"", value);
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("unsupported")
	void shouldThrowSQLFeatureNotSupportedException(String name, Executable function) {
		statement = createStatementWithSql("INSERT INTO cars(make) VALUES ($1)");
		assertThrows(SQLFeatureNotSupportedException.class, function);
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("setNumber")
	<T> void shouldSetNumber(String name, ParameterizedSetter<T> setter, T value) throws SQLException {
		statement = createStatementWithSql("INSERT INTO cars(price) VALUES ($1)");

		setter.set(statement, 1, value);
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), any());
	}

	@Test
	@DefaultTimeZone("Europe/London")
	void shouldSetDate() throws SQLException {
		statement = createStatementWithSql("INSERT INTO cars(release_date) VALUES ($1)");

		statement.setDate(1, new Date(1564527600000L));
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), any());
		assertEquals("INSERT INTO cars(release_date) VALUES ($1)",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
		assertEquals("[{\"name\":\"$1\",\"value\":\"2019-07-31\"}]",
				queryInfoWrapperArgumentCaptor.getValue().getPreparedStatementParameters());
	}

	@Test
	void shouldSetDateWithCalendar() throws SQLException, ParseException {
		statement = createStatementWithSql("INSERT INTO cars(release_date) VALUES ($1)");
		Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("JST"));
		calendar.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z").parse("2024-04-18 20:00:00 GMT"));
		statement.setDate(1, new Date(calendar.getTimeInMillis()), calendar);
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), any());
		assertEquals("INSERT INTO cars(release_date) VALUES ($1)",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
		assertEquals("[{\"name\":\"$1\",\"value\":\"2024-04-19\"}]",
				queryInfoWrapperArgumentCaptor.getValue().getPreparedStatementParameters());
	}

	@Test
	@DefaultTimeZone("Europe/London")
	void shouldSetDateWithNullCalendar() throws SQLException {
		statement = createStatementWithSql("INSERT INTO cars(release_date) VALUES ($1)");

		statement.setDate(1, new Date(1564527600000L), null);
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), any());
		assertEquals("INSERT INTO cars(release_date) VALUES ($1)",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
		assertEquals("[{\"name\":\"$1\",\"value\":\"2019-07-31\"}]",
				queryInfoWrapperArgumentCaptor.getValue().getPreparedStatementParameters());
	}

	@Test
	void shouldSetTimeStampWithCalendar() throws SQLException, ParseException {
		statement = createStatementWithSql("INSERT INTO cars(release_date) VALUES ($1)");
		Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("JST"));
		calendar.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z").parse("2024-04-18 20:11:01 GMT"));
		statement.setTimestamp(1, new Timestamp(calendar.getTimeInMillis()), calendar);
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), any());
		assertEquals("INSERT INTO cars(release_date) VALUES ($1)",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
		assertEquals("[{\"name\":\"$1\",\"value\":\"2024-04-19 05:11:01\"}]",
				queryInfoWrapperArgumentCaptor.getValue().getPreparedStatementParameters());
	}

	@Test
	@DefaultTimeZone("Europe/London")
	void shouldSetTimeStamp() throws SQLException {
		statement = createStatementWithSql("INSERT INTO cars(release_date) VALUES ($1)");

		statement.setTimestamp(1, new Timestamp(1564571713000L));
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), any());
		assertEquals("INSERT INTO cars(release_date) VALUES ($1)",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
		assertEquals("[{\"name\":\"$1\",\"value\":\"2019-07-31 12:15:13.0\"}]",
				queryInfoWrapperArgumentCaptor.getValue().getPreparedStatementParameters());
	}

	@Test
	void shouldSetNullTimeStampWithCalendar() throws SQLException, ParseException {
		statement = createStatementWithSql("INSERT INTO cars(release_date) VALUES ($1)");
		Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("JST"));
		calendar.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z").parse("2024-04-18 20:11:01 GMT"));
		statement.setTimestamp(1, null, calendar);
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), any());
		assertEquals("INSERT INTO cars(release_date) VALUES ($1)",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
		assertEquals("[{\"name\":\"$1\",\"value\":null}]",
				queryInfoWrapperArgumentCaptor.getValue().getPreparedStatementParameters());
	}

	@Test
	@DefaultTimeZone("Europe/London")
	void shouldSetAllObjects() throws SQLException {
		statement = createStatementWithSql(
				"INSERT INTO cars (timestamp, date, float, long, big_decimal, null, boolean, int) " +
						"VALUES ($1,$2,$3,$4,$5,$6,$7,$8)");

		statement.setObject(1, new Timestamp(1564571713000L));
		statement.setObject(2, new Date(1564527600000L));
		statement.setObject(3, 5.5F);
		statement.setObject(4, 5L);
		statement.setObject(5, new BigDecimal("555555555555.55555555"));
		statement.setObject(6, null);
		statement.setObject(7, true);
		statement.setObject(8, 5);

		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), any());
		assertEquals("INSERT INTO cars (timestamp, date, float, long, big_decimal, null, boolean, int) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
		assertEquals("[{\"name\":\"$1\",\"value\":\"2019-07-31 12:15:13.0\"},{\"name\":\"$2\",\"value\":\"2019-07-31\"}," +
						"{\"name\":\"$3\",\"value\":5.5},{\"name\":\"$4\",\"value\":5},{\"name\":\"$5\",\"value\":555555555555.55555555}," +
						"{\"name\":\"$6\",\"value\":null},{\"name\":\"$7\",\"value\":true},{\"name\":\"$8\",\"value\":5}]",
				queryInfoWrapperArgumentCaptor.getValue().getPreparedStatementParameters());
	}

	@Test
	@DefaultTimeZone("Europe/London")
	void shouldSetAllObjectsWithCorrectSqlType() throws SQLException {
		statement = createStatementWithSql(
				"INSERT INTO cars (timestamp, date, float, long, big_decimal, null, boolean, int) " +
						"VALUES ($1,$2,$3,$4,$5,$6,$7,$8)");

		statement.setObject(1, new Timestamp(1564571713000L), Types.TIMESTAMP);
		statement.setObject(2, new Date(1564527600000L), Types.DATE);
		statement.setObject(3, 5.5F, Types.FLOAT);
		statement.setObject(4, 5L, Types.BIGINT);
		statement.setObject(5, new BigDecimal("555555555555.55555555"), Types.NUMERIC);
		statement.setObject(6, null, Types.JAVA_OBJECT);
		statement.setObject(7, true, Types.BOOLEAN);
		statement.setObject(8, 5, Types.INTEGER);

		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), any());

		assertEquals("INSERT INTO cars (timestamp, date, float, long, big_decimal, null, boolean, int) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
		assertEquals("[{\"name\":\"$1\",\"value\":\"2019-07-31 12:15:13.0\"},{\"name\":\"$2\",\"value\":\"2019-07-31\"}," +
						"{\"name\":\"$3\",\"value\":5.5},{\"name\":\"$4\",\"value\":5},{\"name\":\"$5\",\"value\":555555555555.55555555}," +
						"{\"name\":\"$6\",\"value\":null},{\"name\":\"$7\",\"value\":true},{\"name\":\"$8\",\"value\":5}]",
				queryInfoWrapperArgumentCaptor.getValue().getPreparedStatementParameters());
	}

	@Test
	void shouldSetParametersWithRandomIndex() throws SQLException {
		statement = createStatementWithSql("INSERT INTO cars (float, long, null) VALUES ($1,$42,$32)");

		statement.setObject(1, 5.5F, Types.FLOAT);
		statement.setObject(42, 5L, Types.BIGINT);
		statement.setObject(32, null, Types.JAVA_OBJECT);

		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), any());

		assertEquals("INSERT INTO cars (float, long, null) VALUES ($1,$42,$32)",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
		assertEquals("[{\"name\":\"$32\",\"value\":null},{\"name\":\"$1\",\"value\":5.5},{\"name\":\"$42\",\"value\":5}]",
				queryInfoWrapperArgumentCaptor.getValue().getPreparedStatementParameters());
	}

	@Test
	void shouldSetParametersWithRepeatingValues() throws SQLException {
		statement = createStatementWithSql("INSERT INTO cars (float, long, null) VALUES ($1,$1,$1)");

		statement.setObject(1, 5.5F, Types.FLOAT);

		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), any());

		assertEquals("INSERT INTO cars (float, long, null) VALUES ($1,$1,$1)",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
		assertEquals("[{\"name\":\"$1\",\"value\":5.5}]",
				queryInfoWrapperArgumentCaptor.getValue().getPreparedStatementParameters());
	}

	@ParameterizedTest
	@CsvSource(value = {
			"123," + Types.TINYINT + ",3",
			"123," + Types.SMALLINT + ",1",
			"123," + Types.INTEGER + ",",
			"123," + Types.BIGINT + ",",
	})
	// scale is ignored for these types
	void shouldSetIntegerObjectWithCorrectSqlType(int value, int type, Integer scale) throws SQLException {
		shouldSetObjectWithCorrectSqlType(value, type, scale, String.valueOf(value));
	}

	@ParameterizedTest
	@CsvSource(value = {
			"3.14," + Types.DECIMAL + ",2,3.14",
			"3.1415926," + Types.DECIMAL + ",2,3.14",
			"2.7," + Types.NUMERIC + ",2,2.7",
			"2.718281828," + Types.NUMERIC + ",1,2.7",
			"2.718281828," + Types.NUMERIC + ",5,2.71828",
	})
	void shouldSetFloatObjectWithCorrectScalableSqlTypeAndScale(float value, int type, int scale, String expected) throws SQLException {
		shouldSetObjectWithCorrectSqlType(value, type, scale, expected);
	}

	@ParameterizedTest
	@CsvSource(value = {
			"3.14," + Types.DECIMAL + ",2,3.14",
			"3.1415926," + Types.DECIMAL + ",2,3.14",
			"2.7," + Types.NUMERIC + ",2,2.7",
			"2.718281828," + Types.NUMERIC + ",1,2.7",
			"2.718281828," + Types.NUMERIC + ",5,2.71828",
	})
	void shouldSetDoubleObjectWithCorrectScalableSqlTypeAndScale(double value, int type, int scale, String expected) throws SQLException {
		shouldSetObjectWithCorrectSqlType(value, type, scale, expected);
	}

	@ParameterizedTest
	@CsvSource(value = {
			"3.14," + Types.FLOAT + ",",
			"3.1415926," + Types.DOUBLE + ",",
			"3.1415926," + Types.DOUBLE + ",3", // scale is ignored for this type
	})
	void shouldSetDoubleObjectWithCorrectSqlTypeAndScale(double value, int type, Integer scale) throws SQLException {
		shouldSetObjectWithCorrectSqlType(value, type, scale, Double.toString(value));
	}

	@Test
	void unsupportedType() {
		statement = createStatementWithSql("INSERT INTO data (column) VALUES ($1)");
		assertThrows(SQLException.class, () -> statement.setObject(1, this));
		// STRUCT is not supported now, so it can be used as an example of unsupported type
		assertThrows(SQLException.class, () -> statement.setObject(1, "", Types.STRUCT));
		assertThrows(SQLFeatureNotSupportedException.class, () -> statement.setObject(1, "", Types.STRUCT, 5));

		// this test definitely cannot be passed to the prepared statement, so exception is expected here.
		assertThrows(SQLException.class, () -> statement.setObject(1, this, Types.VARCHAR));
		assertThrows(SQLException.class, () -> statement.setObject(1, this));

		// unsupported SQL Type
		assertThrows(SQLException.class, () -> statement.setObject(1, "", 999999));
	}

	private void shouldSetObjectWithCorrectSqlType(Object value, int type, Integer scale, String expected) throws SQLException {
		statement = createStatementWithSql("INSERT INTO data (column) VALUES ($1)");
		if (scale == null) {
			statement.setObject(1, value, type);
		} else {
			statement.setObject(1, value, type, scale);
		}
		statement.execute();
		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), any());

		assertEquals("INSERT INTO data (column) VALUES ($1)", queryInfoWrapperArgumentCaptor.getValue().getSql());
		assertEquals("[{\"name\":\"$1\",\"value\":" + expected + "}]",
				queryInfoWrapperArgumentCaptor.getValue().getPreparedStatementParameters());
	}

	@Test
	void clearParameters() throws SQLException {
		statement = createStatementWithSql("INSERT INTO data (column) VALUES ($1)");
		statement.setObject(1, ""); // set parameter
		statement.execute(); // execute statement - should work because all parameters are set
		statement.clearParameters(); // clear parameters; now there are no parameters
		statement.execute(); // execution passes because we rely on server validation
		verify(fireboltStatementService, times(2)).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), any());
	}

	@Test
	void shouldThrowExceptionWhenExecutedWithSql() throws SQLException {
		statement = createStatementWithSql("SELECT 1");
		statement.execute(); // should work
		assertThrows(SQLException.class, () -> statement.execute("SELECT 1"));
	}

	private FireboltPreparedStatement createStatementWithSql(String sql) {
		return new FireboltBackendPreparedStatement(fireboltStatementService, connection, sql);
	}
}
