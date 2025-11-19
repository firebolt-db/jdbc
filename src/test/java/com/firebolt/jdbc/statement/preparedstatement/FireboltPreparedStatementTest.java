package com.firebolt.jdbc.statement.preparedstatement;

import com.firebolt.jdbc.client.query.StatementClient;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.service.FireboltStatementService;
import com.firebolt.jdbc.statement.StatementInfoWrapper;
import com.firebolt.jdbc.type.FireboltDataType;
import com.firebolt.jdbc.type.array.FireboltArray;
import com.firebolt.jdbc.type.lob.FireboltBlob;
import com.firebolt.jdbc.type.lob.FireboltClob;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
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
import java.time.LocalDate;
import java.time.LocalDateTime;
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

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(MockitoExtension.class)
class FireboltPreparedStatementTest {

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

					// TODO: add support of these methods
					Arguments.of("getParameterMetaData", (Executable) () -> statement.getParameterMetaData())
		);
	}

	private static Stream<Arguments> buffers() {
		return Stream.of(
				Arguments.of("setClob((Clob)null)", (Setter) statement -> statement.setClob(1, (Clob)null), "NULL"),
				Arguments.of("setClob((Reader)null)", (Setter) statement -> statement.setClob(1, (Reader)null), "NULL"),
				Arguments.of("setClob((Reader)null, length)", (Setter) statement -> statement.setClob(1, null, 1L), "NULL"),
				Arguments.of("setClob(Reader)", (Setter) statement -> statement.setClob(1, new FireboltClob("hello".toCharArray())), "'hello'"),
				Arguments.of("setClob(Reader, length=)", (Setter) statement -> statement.setClob(1, new StringReader("hello"), 5), "'hello'"),
				Arguments.of("setClob(Reader, length-1)", (Setter) statement -> statement.setClob(1, new StringReader("hello"), 4), "'hell'"),
				Arguments.of("setClob(Reader, length+1)", (Setter) statement -> statement.setClob(1, new StringReader("hello"), 6), "'hello'"),
				Arguments.of("setClob(Reader, 42)", (Setter) statement -> statement.setClob(1, new StringReader("hello"), 42), "'hello'"),
				Arguments.of("setClob(Reader, 1)", (Setter) statement -> statement.setClob(1, new StringReader("hello"), 1), "'h'"),
				Arguments.of("setClob(Reader, 0)", (Setter) statement -> statement.setClob(1, new StringReader("hello"), 0), "''"),

				Arguments.of("setNClob((NClob)null)", (Setter) statement -> statement.setNClob(1, (NClob)null), "NULL"),
				Arguments.of("setNClob((Reader)null)", (Setter) statement -> statement.setNClob(1, (Reader)null), "NULL"),
				Arguments.of("setNClob((Reader)null, length)", (Setter) statement -> statement.setNClob(1, null, 1L), "NULL"),
				Arguments.of("setClob(Reader)", (Setter) statement -> statement.setNClob(1, new FireboltClob("hello".toCharArray())), "'hello'"),
				Arguments.of("setNClob(Reader, length=)", (Setter) statement -> statement.setNClob(1, new StringReader("hello"), 5), "'hello'"),
				Arguments.of("setNClob(Reader, length-1)", (Setter) statement -> statement.setNClob(1, new StringReader("hello"), 4), "'hell'"),
				Arguments.of("setNClob(Reader, length+1)", (Setter) statement -> statement.setNClob(1, new StringReader("hello"), 6), "'hello'"),
				Arguments.of("setNClob(Reader, 42)", (Setter) statement -> statement.setNClob(1, new StringReader("hello"), 42), "'hello'"),
				Arguments.of("setNClob(Reader, 1)", (Setter) statement -> statement.setNClob(1, new StringReader("hello"), 1), "'h'"),
				Arguments.of("setNClob(Reader, 0)", (Setter) statement -> statement.setNClob(1, new StringReader("hello"), 0), "''"),

				Arguments.of("setBlob((Blob)null)", (Setter) statement -> statement.setBlob(1, (Blob)null), "NULL"),
				Arguments.of("setBClob((InputStream)null)", (Setter) statement -> statement.setBlob(1, (InputStream)null), "NULL"),
				Arguments.of("setBClob((InputStream)null, length)", (Setter) statement -> statement.setBlob(1, null, 1L), "NULL"),
				Arguments.of("setBlob((Clob)null)", (Setter) statement -> statement.setBlob(1, new FireboltBlob("hello".getBytes())), "E'\\x68\\x65\\x6c\\x6c\\x6f'::BYTEA"),

				Arguments.of("setCharacterStream(null)", (Setter) statement -> statement.setCharacterStream(1, null), "NULL"),
				Arguments.of("setCharacterStream(null, int)", (Setter) statement -> statement.setCharacterStream(1, null, 1), "NULL"),
				Arguments.of("setCharacterStream(null, long)", (Setter) statement -> statement.setCharacterStream(1, null, 1L), "NULL"),
				Arguments.of("setCharacterStream(Reader)", (Setter) statement -> statement.setCharacterStream(1, new StringReader("hello")), "'hello'"),
				Arguments.of("setCharacterStream(Reader, length=)", (Setter) statement -> statement.setCharacterStream(1, new StringReader("hello"), 5), "'hello'"),
				Arguments.of("setCharacterStream(Reader, length-1)", (Setter) statement -> statement.setCharacterStream(1, new StringReader("hello"), 4), "'hell'"),
				Arguments.of("setCharacterStream(Reader, length+1)", (Setter) statement -> statement.setCharacterStream(1, new StringReader("hello"), 6), "'hello'"),
				Arguments.of("setCharacterStream(Reader, 42)", (Setter) statement -> statement.setCharacterStream(1, new StringReader("hello"), 42), "'hello'"),
				Arguments.of("setCharacterStream(Reader, 1)", (Setter) statement -> statement.setCharacterStream(1, new StringReader("hello"), 1), "'h'"),
				Arguments.of("setCharacterStream(Reader, 0)", (Setter) statement -> statement.setCharacterStream(1, new StringReader("hello"), 0), "''"),

				Arguments.of("setNCharacterStream(null)", (Setter) statement -> statement.setNCharacterStream(1, null), "NULL"),
				Arguments.of("setNCharacterStream(null, int)", (Setter) statement -> statement.setNCharacterStream(1, null, 1), "NULL"),
				Arguments.of("setNCharacterStream(null, long)", (Setter) statement -> statement.setNCharacterStream(1, null, 1L), "NULL"),
				Arguments.of("setNCharacterStream(Reader)", (Setter) statement -> statement.setNCharacterStream(1, new StringReader("hello")), "'hello'"),
				Arguments.of("setNCharacterStream(Reader, length=)", (Setter) statement -> statement.setNCharacterStream(1, new StringReader("hello"), 5), "'hello'"),
				Arguments.of("setNCharacterStream(Reader, length-1)", (Setter) statement -> statement.setNCharacterStream(1, new StringReader("hello"), 4), "'hell'"),
				Arguments.of("setNCharacterStream(Reader, length+1)", (Setter) statement -> statement.setNCharacterStream(1, new StringReader("hello"), 6), "'hello'"),
				Arguments.of("setNCharacterStream(Reader, 42)", (Setter) statement -> statement.setNCharacterStream(1, new StringReader("hello"), 42), "'hello'"),
				Arguments.of("setNCharacterStream(Reader, 1)", (Setter) statement -> statement.setNCharacterStream(1, new StringReader("hello"), 1), "'h'"),
				Arguments.of("setNCharacterStream(Reader, 0)", (Setter) statement -> statement.setNCharacterStream(1, new StringReader("hello"), 0), "''"),

				Arguments.of("setAsciiStream(null)", (Setter) statement -> statement.setAsciiStream(1, null), "NULL"),
				Arguments.of("setAsciiStream(null, int)", (Setter) statement -> statement.setAsciiStream(1, null, 1), "NULL"),
				Arguments.of("setAsciiStream(null, long)", (Setter) statement -> statement.setAsciiStream(1, null, 1L), "NULL"),
				Arguments.of("setAsciiStream(InputStream)", (Setter) statement -> statement.setAsciiStream(1, new ByteArrayInputStream("hello".getBytes())), "E'\\x68\\x65\\x6c\\x6c\\x6f'::BYTEA"),
				Arguments.of("setAsciiStream(InputStream, length=)", (Setter) statement -> statement.setAsciiStream(1, new ByteArrayInputStream("hello".getBytes()), 5), "E'\\x68\\x65\\x6c\\x6c\\x6f'::BYTEA"),
				Arguments.of("setAsciiStream(InputStream, length-1)", (Setter) statement -> statement.setAsciiStream(1, new ByteArrayInputStream("hello".getBytes()), 4), "E'\\x68\\x65\\x6c\\x6c'::BYTEA"),
				Arguments.of("setAsciiStream(InputStream, length+1)", (Setter) statement -> statement.setAsciiStream(1, new ByteArrayInputStream("hello".getBytes()), 6), "E'\\x68\\x65\\x6c\\x6c\\x6f'::BYTEA"),
				Arguments.of("setAsciiStream(InputStream, 42)", (Setter) statement -> statement.setAsciiStream(1, new ByteArrayInputStream("hello".getBytes()), 42), "E'\\x68\\x65\\x6c\\x6c\\x6f'::BYTEA"),
				Arguments.of("setAsciiStream(InputStream, 1)", (Setter) statement -> statement.setAsciiStream(1, new ByteArrayInputStream("hello".getBytes()), 1), "E'\\x68'::BYTEA"),
				Arguments.of("setAsciiStream(InputStream, 0)", (Setter) statement -> statement.setAsciiStream(1, new ByteArrayInputStream("hello".getBytes()), 0), "E'\\x'::BYTEA"),

				Arguments.of("setBinaryStream(null)", (Setter) statement -> statement.setBinaryStream(1, null), "NULL"),
				Arguments.of("setBinaryStream(null, int)", (Setter) statement -> statement.setBinaryStream(1, null, 1), "NULL"),
				Arguments.of("setBinaryStream(null, long)", (Setter) statement -> statement.setBinaryStream(1, null, 1L), "NULL"),
				Arguments.of("setBinaryStream(InputStream)", (Setter) statement -> statement.setBinaryStream(1, new ByteArrayInputStream("hello".getBytes())), "E'\\x68\\x65\\x6c\\x6c\\x6f'::BYTEA"),
				Arguments.of("setBinaryStream(InputStream, length=)", (Setter) statement -> statement.setBinaryStream(1, new ByteArrayInputStream("hello".getBytes()), 5), "E'\\x68\\x65\\x6c\\x6c\\x6f'::BYTEA"),
				Arguments.of("setBinaryStream(InputStream, length-1)", (Setter) statement -> statement.setBinaryStream(1, new ByteArrayInputStream("hello".getBytes()), 4), "E'\\x68\\x65\\x6c\\x6c'::BYTEA"),
				Arguments.of("setBinaryStream(InputStream, length+1)", (Setter) statement -> statement.setBinaryStream(1, new ByteArrayInputStream("hello".getBytes()), 6), "E'\\x68\\x65\\x6c\\x6c\\x6f'::BYTEA"),
				Arguments.of("setBinaryStream(InputStream, 42)", (Setter) statement -> statement.setBinaryStream(1, new ByteArrayInputStream("hello".getBytes()), 42), "E'\\x68\\x65\\x6c\\x6c\\x6f'::BYTEA"),
				Arguments.of("setBinaryStream(InputStream, 1)", (Setter) statement -> statement.setBinaryStream(1, new ByteArrayInputStream("hello".getBytes()), 1), "E'\\x68'::BYTEA"),
				Arguments.of("setBinaryStream(InputStream, 0)", (Setter) statement -> statement.setBinaryStream(1, new ByteArrayInputStream("hello".getBytes()), 0), "E'\\x'::BYTEA"),

				Arguments.of("setUnicodeStream(null, int)", (Setter) statement -> statement.setUnicodeStream(1, null, 1), "NULL"),
				Arguments.of("setUnicodeStream(InputStream, length=)", (Setter) statement -> statement.setUnicodeStream(1, new ByteArrayInputStream("hello".getBytes()), 5), "E'\\x68\\x65\\x6c\\x6c\\x6f'::BYTEA"),
				Arguments.of("setUnicodeStream(InputStream, length-1)", (Setter) statement -> statement.setUnicodeStream(1, new ByteArrayInputStream("hello".getBytes()), 4), "E'\\x68\\x65\\x6c\\x6c'::BYTEA"),
				Arguments.of("setUnicodeStream(InputStream, length+1)", (Setter) statement -> statement.setUnicodeStream(1, new ByteArrayInputStream("hello".getBytes()), 6), "E'\\x68\\x65\\x6c\\x6c\\x6f'::BYTEA"),
				Arguments.of("setUnicodeStream(InputStream, 42)", (Setter) statement -> statement.setUnicodeStream(1, new ByteArrayInputStream("hello".getBytes()), 42), "E'\\x68\\x65\\x6c\\x6c\\x6f'::BYTEA"),
				Arguments.of("setUnicodeStream(InputStream, 1)", (Setter) statement -> statement.setUnicodeStream(1, new ByteArrayInputStream("hello".getBytes()), 1), "E'\\x68'::BYTEA"),
				Arguments.of("setUnicodeStream(InputStream, 0)", (Setter) statement -> statement.setUnicodeStream(1, new ByteArrayInputStream("hello".getBytes()), 0), "E'\\x'::BYTEA"));
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
		lenient().when(properties.getPreparedStatementParamStyle()).thenReturn("native");
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
			"INSERT INTO data (field) VALUES (?),false",
			"SELECT * FROM data WHERE field=?,true",
	})
	void getMetadata(String query, boolean expectedResultSet) throws SQLException {
		StatementClient statementClient = mock(StatementClient.class);
		when(statementClient.executeSqlStatement(any(), any(), anyBoolean(), anyInt(), eq(false))).thenReturn(new ByteArrayInputStream(new byte[0]));
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
		statement = createStatementWithSql("INSERT INTO cars (sales, make, model, minor_model, color, type, types, signature) VALUES (?,?,?,?,?,?,?,?)");

		statement.setInt(1, 500);
		statement.setString(2, "Ford");
		statement.setObject(3, "FOCUS", Types.VARCHAR);
		statement.setNull(4, Types.VARCHAR);
		statement.setNull(5, Types.VARCHAR,  "VARCHAR");
		statement.setNString(6, "sedan");
		statement.setArray(7, new FireboltArray(FireboltDataType.TEXT, new String[] {"sedan", "hatchback", "coupe"}));
		statement.setBytes(8, "HarryFord".getBytes());
		statement.execute();
		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), any());
		assertEquals("INSERT INTO cars (sales, make, model, minor_model, color, type, types, signature) VALUES (500,'Ford','FOCUS',NULL,NULL,'sedan',['sedan','hatchback','coupe'],E'\\x48\\x61\\x72\\x72\\x79\\x46\\x6f\\x72\\x64'::BYTEA)",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	@Test
	void setNullByteArray() throws SQLException {
		statement = createStatementWithSql("INSERT INTO cars (sales, make, model, minor_model, color, type, types, signature) VALUES (?,?,?,?,?,?,?,?)");

		statement.setShort(1, (short)500);
		statement.setString(2, "Ford");
		statement.setObject(3, "FOCUS", Types.VARCHAR);
		statement.setNull(4, Types.VARCHAR);
		statement.setNull(5, Types.VARCHAR,  "VARCHAR");
		statement.setNString(6, "sedan");
		statement.setArray(7, new FireboltArray(FireboltDataType.TEXT, new String[] {"sedan", "hatchback", "coupe"}));
		statement.setBytes(8, null);
		statement.execute();
		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), any());
		assertEquals("INSERT INTO cars (sales, make, model, minor_model, color, type, types, signature) VALUES (500,'Ford','FOCUS',NULL,NULL,'sedan',['sedan','hatchback','coupe'],NULL)",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("buffers")
	void setBuffer(String name, Setter setter, String expected) throws SQLException {
		statement = createStatementWithSql("INSERT INTO data (field) VALUES (?)");
		setter.set(statement);
		statement.execute();
		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), any());
		assertEquals(format("INSERT INTO data (field) VALUES (%s)", expected), queryInfoWrapperArgumentCaptor.getValue().getSql());
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
		statement = createStatementWithSql("INSERT INTO cars (sales, make) VALUES (?,?)");

		statement.setObject(1, 150);
		statement.setObject(2, "Ford");
		statement.addBatch();
		statement.setObject(1, 300);
		statement.setObject(2, "Tesla");
		statement.addBatch();
		statement.executeBatch();
		verify(fireboltStatementService, times(2)).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), any());
		assertEquals("INSERT INTO cars (sales, make) VALUES (150,'Ford')",
				queryInfoWrapperArgumentCaptor.getAllValues().get(0).getSql());
		assertEquals("INSERT INTO cars (sales, make) VALUES (300,'Tesla')",
				queryInfoWrapperArgumentCaptor.getAllValues().get(1).getSql());
	}

	@ParameterizedTest
	@ValueSource(strings = {
			"SELECT * FROM cars WHERE make LIKE ?",
			"SELECT * FROM cars WHERE make LIKE ,?",
			"SELECT * FROM cars WHERE make LIKE (?"
	})
	void shouldThrowExceptionWhenAllParametersAreNotDefined(String query) throws SQLException {
		try (PreparedStatement ps = createStatementWithSql(query)) {
			assertThrows(IllegalArgumentException.class, ps::executeQuery);
		}
	}

	@Test
	void shouldExecuteWithSpecialCharactersInQuery() throws SQLException {
		String sql = "INSERT INTO cars (model ,sales, make) VALUES (?,?,'(?:^|[^\\\\p{L}\\\\p{N}])(?i)(phone)(?:[^\\\\p{L}\\\\p{N}]|$)')";
		statement = createStatementWithSql(sql);

		statement.setObject(1, "?");
		statement.setObject(2, " ?");

		String expectedSql = "INSERT INTO cars (model ,sales, make) VALUES ('?',' ?','(?:^|[^\\\\p{L}\\\\p{N}])(?i)(phone)(?:[^\\\\p{L}\\\\p{N}]|$)')";

		statement.execute();
		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), any());
		assertEquals(expectedSql, queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	@Test
	void shouldThrowExceptionWhenTooManyParametersAreProvided() throws SQLException {
		String sql = "INSERT INTO cars (model ,sales, make) VALUES (?, '(?:^|[^\\\\p{L}\\\\p{N}])(?i)(phone)(?:[^\\\\p{L}\\\\p{N}]|$)')";
		statement  = createStatementWithSql(sql);

		statement.setObject(1, "A");

		assertThrows(FireboltException.class, () -> statement.setObject(2, "B"));
	}

	@Test
	void shouldExecuteUpdate() throws SQLException {
		statement  = createStatementWithSql("update cars set sales = ? where make = ?");

		statement.setObject(1, 150);
		statement.setObject(2, "Ford");

		assertEquals(0, statement.executeUpdate()); // we are not able to return number of affected lines right now

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), any());
		assertEquals("update cars set sales = 150 where make = 'Ford'",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
	}


	@Test
	void shouldThrowsExceptionWhenTryingToExecuteUpdateWithQuery() throws SQLException {
		statement  = createStatementWithSql("update cars set sales = ? where make = ?");

		statement.setObject(1, 150);
		statement.setObject(2, "Ford");

		assertThrows(FireboltException.class,
				() -> statement.executeUpdate("update cars set sales = 50 where make = 'Ford"));
	}

	@Test
	void shouldSetNull() throws SQLException {
		statement = createStatementWithSql("INSERT INTO cars (sales, make) VALUES (?,?)");

		statement.setNull(1, 0);
		statement.setNull(2, 0);
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), any());
		assertEquals("INSERT INTO cars (sales, make) VALUES (NULL,NULL)",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	@Test
	void shouldSetBoolean() throws SQLException {
		statement = createStatementWithSql("INSERT INTO cars(available) VALUES (?)");

		statement.setBoolean(1, true);
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), any());
		assertEquals("INSERT INTO cars(available) VALUES (true)", queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	@ParameterizedTest
	@CsvSource(value = {
			"Nobody,",
			"Firebolt,http://www.firebolt.io"
	})
	void shouldSetUrl(String name, URL url) throws SQLException {
		statement = createStatementWithSql("INSERT INTO companies (name, url) VALUES (?,?)");

		statement.setString(1, name);
		statement.setURL(2, url);
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), any());
		assertEquals(format("INSERT INTO companies (name, url) VALUES (%s,%s)", sqlQuote(name), sqlQuote(url)), queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	private String sqlQuote(Object value) {
		return value == null ? "NULL" : format("'%s'", value);
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("unsupported")
	void shouldThrowSQLFeatureNotSupportedException(String name, Executable function) {
		statement = createStatementWithSql("INSERT INTO cars(make) VALUES (?)");
		assertThrows(SQLFeatureNotSupportedException.class, function);
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("setNumber")
	<T> void shouldSetNumber(String name, ParameterizedSetter<T> setter, T value) throws SQLException {
		statement = createStatementWithSql("INSERT INTO cars(price) VALUES (?)");

		setter.set(statement, 1, value);
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), any());
	}

	@Test
	@DefaultTimeZone("Europe/London")
	void shouldSetDate() throws SQLException {
		statement = createStatementWithSql("INSERT INTO cars(release_date) VALUES (?)");

		statement.setDate(1, new Date(1564527600000L));
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), any());
		assertEquals("INSERT INTO cars(release_date) VALUES ('2019-07-31')",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	@Test
	void shouldSetDateWithCalendar() throws SQLException, ParseException {
		statement = createStatementWithSql("INSERT INTO cars(release_date) VALUES (?)");
		Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("JST"));
		calendar.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z").parse("2024-04-18 20:00:00 GMT"));
		statement.setDate(1, new Date(calendar.getTimeInMillis()), calendar);
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), any());
		assertEquals("INSERT INTO cars(release_date) VALUES ('2024-04-19')",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	@Test
	@DefaultTimeZone("Europe/London")
	void shouldSetDateWithNullCalendar() throws SQLException, ParseException {
		statement = createStatementWithSql("INSERT INTO cars(release_date) VALUES (?)");

		statement.setDate(1, new Date(1564527600000L), null);
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), any());
		assertEquals("INSERT INTO cars(release_date) VALUES ('2019-07-31')",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	@Test
	void shouldSetTimeStampWithCalendar() throws SQLException, ParseException {
		statement = createStatementWithSql("INSERT INTO cars(release_date) VALUES (?)");
		Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("JST"));
		calendar.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z").parse("2024-04-18 20:11:01 GMT"));
		statement.setTimestamp(1, new Timestamp(calendar.getTimeInMillis()), calendar);
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), any());
		assertEquals("INSERT INTO cars(release_date) VALUES ('2024-04-19 05:11:01')",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	@Test
	@DefaultTimeZone("Europe/London")
	void shouldSetTimeStamp() throws SQLException {
		statement = createStatementWithSql("INSERT INTO cars(release_date) VALUES (?)");

		statement.setTimestamp(1, new Timestamp(1564571713000L));
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), any());
		assertEquals("INSERT INTO cars(release_date) VALUES ('2019-07-31 12:15:13')",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	@Test
	void shouldSetNullTimeStampWithCalendar() throws SQLException, ParseException {
		statement = createStatementWithSql("INSERT INTO cars(release_date) VALUES (?)");
		Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("JST"));
		calendar.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z").parse("2024-04-18 20:11:01 GMT"));
		statement.setTimestamp(1, null, calendar);
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), any());
		assertEquals("INSERT INTO cars(release_date) VALUES (NULL)",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	@ParameterizedTest
	@DefaultTimeZone("Europe/London")
	@MethodSource("dateTypes")
	void shouldSetAllObjects(Object timestampOrLocalDateTime, Object dateOrLocalDate) throws SQLException {
		statement = createStatementWithSql(
				"INSERT INTO cars(timestamp, date, float, long, big_decimal, null, boolean, int) "
						+ "VALUES (?,?,?,?,?,?,?,?)");

		statement.setObject(1, timestampOrLocalDateTime);
		statement.setObject(2, dateOrLocalDate);
		statement.setObject(3, 5.5F);
		statement.setObject(4, 5L);
		statement.setObject(5, new BigDecimal("555555555555.55555555"));
		statement.setObject(6, null);
		statement.setObject(7, true);
		statement.setObject(8, 5);

		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), any());
		assertEquals(
				"INSERT INTO cars(timestamp, date, float, long, big_decimal, null, boolean, int) VALUES ('2019-07-31 12:15:13','2019-07-31',5.5,5,555555555555.55555555,NULL,true,5)",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	@ParameterizedTest
	@DefaultTimeZone("Europe/London")
	@MethodSource("dateTypes")
	void shouldSetAllObjectsWithCorrectSqlType(Object timestampOrLocalDateTime, Object dateOrLocalDate) throws SQLException {
		statement = createStatementWithSql(
				"INSERT INTO cars(timestamp, date, float, long, big_decimal, null, boolean, int) "
						+ "VALUES (?,?,?,?,?,?,?,?)");

		statement.setObject(1, timestampOrLocalDateTime, Types.TIMESTAMP);
		statement.setObject(2, dateOrLocalDate, Types.DATE);
		statement.setObject(3, 5.5F, Types.FLOAT);
		statement.setObject(4, 5L, Types.BIGINT);
		statement.setObject(5, new BigDecimal("555555555555.55555555"), Types.NUMERIC);
		statement.setObject(6, null, Types.JAVA_OBJECT);
		statement.setObject(7, true, Types.BOOLEAN);
		statement.setObject(8, 5, Types.INTEGER);

		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), any());

		assertEquals(
				"INSERT INTO cars(timestamp, date, float, long, big_decimal, null, boolean, int) VALUES ('2019-07-31 12:15:13','2019-07-31',5.5,5,555555555555.55555555,NULL,true,5)",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
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
			"2.7," + Types.NUMERIC + ",2,2.70",
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
			"2.7," + Types.NUMERIC + ",2,2.70",
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
		statement = createStatementWithSql("INSERT INTO data (column) VALUES (?)");
		assertThrows(SQLException.class, () -> statement.setObject(1, this));
		// STRUCT is not supported now, so it can be used as an example of unsupported type
		assertThrows(SQLFeatureNotSupportedException.class, () -> statement.setObject(1, "", Types.STRUCT));
		assertThrows(SQLFeatureNotSupportedException.class, () -> statement.setObject(1, "", Types.STRUCT, 5));

		// this test definitely cannot be passed to the prepared statement, so exception is expected here.
		assertThrows(SQLException.class, () -> statement.setObject(1, this, Types.VARCHAR));
		assertThrows(SQLException.class, () -> statement.setObject(1, this));

		// unsupported SQL Type
		assertThrows(SQLFeatureNotSupportedException.class, () -> statement.setObject(1, "", 999999));
	}

	private void shouldSetObjectWithCorrectSqlType(Object value, int type, Integer scale, String expected) throws SQLException {
		statement = createStatementWithSql("INSERT INTO data (column) VALUES (?)");
		if (scale == null) {
			statement.setObject(1, value, type);
		} else {
			statement.setObject(1, value, type, scale);
		}
		statement.execute();
		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), any());

		assertEquals(format("INSERT INTO data (column) VALUES (%s)", expected), queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	@Test
	void clearParameters() throws SQLException {
		statement = createStatementWithSql("INSERT INTO data (column) VALUES (?)");
		statement.setObject(1, ""); // set parameter
		statement.execute(); // execute statement - should work because all parameters are set
		statement.clearParameters(); // clear parameters; now there are no parameters
		assertThrows(IllegalArgumentException.class, () -> statement.execute()); // execution fails because parameters are not set
		statement.setObject(1, ""); // set parameter again
		statement.execute(); // now execution is successful
	}

	@Test
	void shouldThrowExceptionWhenExecutedWithSql() throws SQLException {
		statement = createStatementWithSql("SELECT 1");
		statement.execute(); // should work
		assertThrows(SQLException.class, () -> statement.execute("SELECT 1"));
	}

	private FireboltPreparedStatement createStatementWithSql(String sql) {
		return new FireboltPreparedStatement(fireboltStatementService, connection, sql);
	}

	Stream<Arguments> dateTypes() {
		return Stream.of(
				Arguments.of(LocalDateTime.of(2019, 7, 31, 12, 15, 13),
						LocalDate.of(2019, 7, 31)),
				Arguments.of(new Timestamp(1564571713000L),
						new Date(1564527600000L))
		);
	}
}
