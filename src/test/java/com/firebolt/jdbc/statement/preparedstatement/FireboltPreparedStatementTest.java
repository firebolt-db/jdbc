package com.firebolt.jdbc.statement.preparedstatement;

import com.firebolt.jdbc.client.query.StatementClient;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.service.FireboltStatementService;
import com.firebolt.jdbc.statement.StatementInfoWrapper;
import com.firebolt.jdbc.type.FireboltDataType;
import com.firebolt.jdbc.type.array.FireboltArray;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
import java.io.InputStream;
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
import java.util.Calendar;
import java.util.Optional;
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
class FireboltPreparedStatementTest {

	@Captor
	ArgumentCaptor<StatementInfoWrapper> queryInfoWrapperArgumentCaptor;
	@Mock
	private FireboltStatementService fireboltStatementService;
	@Mock
	private FireboltProperties properties;

	private final FireboltConnection connection = Mockito.mock(FireboltConnection.class);
	private static FireboltPreparedStatement statement;

	private static Stream<Arguments> unsupported() {
		return Stream.of(
					Arguments.of("setByte", (Executable) () -> statement.setByte(1, (byte) 127)),
					Arguments.of("setURL", (Executable) () -> statement.setURL(1, new URL("http://foo.bar"))),
					Arguments.of("setCharacterStream", (Executable) () -> statement.setCharacterStream(1, new StringReader("hello"))),
					Arguments.of("setCharacterStream(length)", (Executable) () -> statement.setCharacterStream(1, new StringReader("hello"), 2)),
					Arguments.of("setCharacterStream(long length)", (Executable) () -> statement.setCharacterStream(1, new StringReader("hello"), 2L)),
					Arguments.of("setNCharacterStream", (Executable) () -> statement.setNCharacterStream(1, new StringReader("hello"))),
					Arguments.of("setNCharacterStream(length)", (Executable) () -> statement.setNCharacterStream(1, new StringReader("hello"), 2)),
					Arguments.of("setNCharacterStream(length)", (Executable) () -> statement.setNCharacterStream(1, new StringReader("hello"), 2L)),

					Arguments.of("setRef", (Executable) () -> statement.setRef(1, mock(Ref.class))),
					Arguments.of("setBlob", (Executable) () -> statement.setBlob(1, mock(Blob.class))),
					Arguments.of("setBlob(input stream)", (Executable) () -> statement.setBlob(1, mock(InputStream.class))),
					Arguments.of("setBlob(input stream, length)", (Executable) () -> statement.setBlob(1, mock(InputStream.class), 123)),
					Arguments.of("setClob", (Executable) () -> statement.setClob(1, mock(Clob.class))),
					Arguments.of("setClob(reader)", (Executable) () -> statement.setClob(1, new StringReader("hello"))),
					Arguments.of("setClob(reader, length)", (Executable) () -> statement.setClob(1, new StringReader("hello"), 1)),
					Arguments.of("setNClob", (Executable) () -> statement.setNClob(1, mock(NClob.class))),
					Arguments.of("setNClob(reader)", (Executable) () -> statement.setNClob(1, new StringReader("hello"))),
					Arguments.of("setNClob(reader, length)", (Executable) () -> statement.setNClob(1, new StringReader("hello"), 1L)),

					Arguments.of("setAsciiStream", (Executable) () -> statement.setAsciiStream(1, mock(InputStream.class))),
					Arguments.of("setAsciiStream(int length)", (Executable) () -> statement.setAsciiStream(1, mock(InputStream.class), 456)),
					Arguments.of("setAsciiStream(long length)", (Executable) () -> statement.setAsciiStream(1, mock(InputStream.class), 456L)),
					Arguments.of("setBinaryStream", (Executable) () -> statement.setBinaryStream(1, mock(InputStream.class))),
					Arguments.of("setBinaryStream(int length)", (Executable) () -> statement.setBinaryStream(1, mock(InputStream.class), 456)),
					Arguments.of("setBinaryStream(long length)", (Executable) () -> statement.setBinaryStream(1, mock(InputStream.class), 456L)),
					Arguments.of("setUnicodeStream(int length)", (Executable) () -> statement.setUnicodeStream(1, mock(InputStream.class), 123)),

					Arguments.of("setDate", (Executable) () -> statement.setDate(1, new Date(System.currentTimeMillis()), Calendar.getInstance())),
					Arguments.of("setTime", (Executable) () -> statement.setTime(1, new Time(System.currentTimeMillis()))),
					Arguments.of("setTime(calendar)", (Executable) () -> statement.setTime(1, new Time(System.currentTimeMillis()), Calendar.getInstance())),
					Arguments.of("setTimestamp", (Executable) () -> statement.setTimestamp(1, new Timestamp(System.currentTimeMillis()), Calendar.getInstance())),
					Arguments.of("setRowId", (Executable) () -> statement.setRowId(1, mock(RowId.class))),
					Arguments.of("setSQLXML", (Executable) () -> statement.setSQLXML(1, mock(SQLXML.class))),

					// TODO: add support of these methods
					Arguments.of("getParameterMetaData", (Executable) () -> statement.getParameterMetaData())
		);
	}

	@BeforeEach
	void beforeEach() throws SQLException {
		when(connection.getSessionProperties()).thenReturn(properties);
		lenient().when(properties.getBufferSize()).thenReturn(10);
		lenient().when(fireboltStatementService.execute(any(), any(), anyBoolean(), any())).thenReturn(Optional.empty());
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
		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), anyBoolean(), any());
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
		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), anyBoolean(), any());
		assertEquals("INSERT INTO cars (sales, make, model, minor_model, color, type, types, signature) VALUES (500,'Ford','FOCUS',NULL,NULL,'sedan',['sedan','hatchback','coupe'],NULL)",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
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
		verify(fireboltStatementService, times(2)).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), anyBoolean(), any());
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
		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), anyBoolean(), any());
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
	void shouldThrowsExceptionWhenTryingToExecuteUpdate() throws SQLException {
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

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), anyBoolean(), any());
		assertEquals("INSERT INTO cars (sales, make) VALUES (NULL,NULL)",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	@Test
	void shouldSetBoolean() throws SQLException {
		statement = createStatementWithSql("INSERT INTO cars(available) VALUES (?)");

		statement.setBoolean(1, true);
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), anyBoolean(), any());
		assertEquals("INSERT INTO cars(available) VALUES (1)", queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	@Test
	void shouldThrowExceptionWhenTryingToSetCharacterStream() {
		statement = createStatementWithSql("INSERT INTO cars(make) VALUES (?)");
		assertThrows(SQLFeatureNotSupportedException.class, () -> statement.setCharacterStream(1, new StringReader("hello")));
		assertThrows(SQLFeatureNotSupportedException.class, () -> statement.setCharacterStream(1, new StringReader("hello"), 2));
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("unsupported")
	void shouldThrowSQLFeatureNotSupportedException(String name, Executable function) {
		statement = createStatementWithSql("INSERT INTO cars(make) VALUES (?)");
		assertThrows(SQLFeatureNotSupportedException.class, function);
	}

	@Test
	void shouldSetInt() throws SQLException {
		statement = createStatementWithSql("INSERT INTO cars(price) VALUES (?)");

		statement.setInt(1, 50);
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), anyBoolean(), any());
	}

	@Test
	void shouldSetLong() throws SQLException {
		statement = createStatementWithSql("INSERT INTO cars(price) VALUES (?)");

		statement.setLong(1, 50L);
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), anyBoolean(), any());
		assertEquals("INSERT INTO cars(price) VALUES (50)", queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	@Test
	void shouldSetFloat() throws SQLException {
		statement = createStatementWithSql("INSERT INTO cars(price) VALUES (?)");

		statement.setFloat(1, 5.5F);
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), anyBoolean(), any());
		assertEquals("INSERT INTO cars(price) VALUES (5.5)", queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	@Test
	void shouldSetDouble() throws SQLException {
		statement = createStatementWithSql("INSERT INTO cars(price) VALUES (?)");

		statement.setDouble(1, 5.5);
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), anyBoolean(), any());
	}

	@Test
	void shouldSetBigDecimal() throws SQLException {
		statement = createStatementWithSql("INSERT INTO cars(price) VALUES (?)");

		statement.setBigDecimal(1, new BigDecimal("555555555555.55555555"));
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), anyBoolean(), any());
		assertEquals("INSERT INTO cars(price) VALUES (555555555555.55555555)",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	@Test
	@DefaultTimeZone("Europe/London")
	void shouldSetDate() throws SQLException {
		statement = createStatementWithSql("INSERT INTO cars(release_date) VALUES (?)");

		statement.setDate(1, new Date(1564527600000L));
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), anyBoolean(), any());
		assertEquals("INSERT INTO cars(release_date) VALUES ('2019-07-31')",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	@Test
	@DefaultTimeZone("Europe/London")
	void shouldSetTimeStamp() throws SQLException {
		statement = createStatementWithSql("INSERT INTO cars(release_date) VALUES (?)");

		statement.setTimestamp(1, new Timestamp(1564571713000L));
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), anyBoolean(), any());
		assertEquals("INSERT INTO cars(release_date) VALUES ('2019-07-31 12:15:13')",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	@Test
	@DefaultTimeZone("Europe/London")
	void shouldSetAllObjects() throws SQLException {
		statement = createStatementWithSql(
				"INSERT INTO cars(timestamp, date, float, long, big_decimal, null, boolean, int) "
						+ "VALUES (?,?,?,?,?,?,?,?)");

		statement.setObject(1, new Timestamp(1564571713000L));
		statement.setObject(2, new Date(1564527600000L));
		statement.setObject(3, 5.5F);
		statement.setObject(4, 5L);
		statement.setObject(5, new BigDecimal("555555555555.55555555"));
		statement.setObject(6, null);
		statement.setObject(7, true);
		statement.setObject(8, 5);

		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), anyBoolean(), any());
		assertEquals(
				"INSERT INTO cars(timestamp, date, float, long, big_decimal, null, boolean, int) VALUES ('2019-07-31 12:15:13','2019-07-31',5.5,5,555555555555.55555555,NULL,1,5)",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	@Test
	@DefaultTimeZone("Europe/London")
	void shouldSetAllObjectsWithCorrectSqlType() throws SQLException {
		statement = createStatementWithSql(
				"INSERT INTO cars(timestamp, date, float, long, big_decimal, null, boolean, int) "
						+ "VALUES (?,?,?,?,?,?,?,?)");

		statement.setObject(1, new Timestamp(1564571713000L), Types.TIMESTAMP);
		statement.setObject(2, new Date(1564527600000L), Types.DATE);
		statement.setObject(3, 5.5F, Types.FLOAT);
		statement.setObject(4, 5L, Types.BIGINT);
		statement.setObject(5, new BigDecimal("555555555555.55555555"), Types.NUMERIC);
		statement.setObject(6, null, Types.JAVA_OBJECT);
		statement.setObject(7, true, Types.BOOLEAN);
		statement.setObject(8, 5, Types.INTEGER);

		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), anyBoolean(), any());

		assertEquals(
				"INSERT INTO cars(timestamp, date, float, long, big_decimal, null, boolean, int) VALUES ('2019-07-31 12:15:13','2019-07-31',5.5,5,555555555555.55555555,NULL,1,5)",
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
		assertThrows(SQLFeatureNotSupportedException.class, () -> statement.setObject(1, this, Types.STRUCT));
		assertThrows(SQLFeatureNotSupportedException.class, () -> statement.setObject(1, this, Types.STRUCT, 5));
	}

	private void shouldSetObjectWithCorrectSqlType(Object value, int type, Integer scale, String expected) throws SQLException {
		statement = createStatementWithSql("INSERT INTO data (column) VALUES (?)");
		if (scale == null) {
			statement.setObject(1, value, type);
		} else {
			statement.setObject(1, value, type, scale);
		}
		statement.execute();
		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(properties), anyBoolean(), any());

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

	private FireboltPreparedStatement createStatementWithSql(String sql) {
		return new FireboltPreparedStatement(fireboltStatementService, connection, sql);
	}
}
