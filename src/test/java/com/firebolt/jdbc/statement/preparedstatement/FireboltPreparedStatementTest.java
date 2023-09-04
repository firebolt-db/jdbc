package com.firebolt.jdbc.statement.preparedstatement;

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
import org.junit.jupiter.params.provider.MethodSource;
import org.junitpioneer.jupiter.DefaultTimeZone;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.InputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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
					Arguments.of("setBytes", (Executable) () -> statement.setBytes(1, "bytes".getBytes())),
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
					Arguments.of("setRowId", (Executable) () -> statement.setRowId(1, mock(RowId.class)),
					Arguments.of("setSQLXML", (Executable) () -> statement.setSQLXML(1, mock(SQLXML.class))),

					// TODO: add support of  this method
					Arguments.of("getParameterMetaData", (Executable) () -> statement.getParameterMetaData())),
					Arguments.of("setObject", (Executable) () -> statement.setObject(1, mock(SQLXML.class), Types.VARCHAR, 0)),
					Arguments.of("getParameterMetaData", (Executable) () -> statement.getParameterMetaData())
		);
	}

	@BeforeEach
	void beforeEach() throws SQLException {
		lenient().when(properties.getBufferSize()).thenReturn(10);
		lenient().when(fireboltStatementService.execute(any(), any(), anyInt(), anyInt(), anyBoolean(), any()))
				.thenReturn(Optional.empty());
	}

	@AfterEach
	void afterEach() throws SQLException {
		if (statement != null) {
			statement.close();
		}
	}

	@Test
	void shouldExecute() throws SQLException {
		statement = createStatementWithSql("INSERT INTO cars (sales, make, model, minor_model, color, type, types) VALUES (?,?,?,?,?,?,?)");

		statement.setInt(1, 500);
		statement.setString(2, "Ford");
		statement.setObject(3, "FOCUS", Types.VARCHAR);
		statement.setNull(4, Types.VARCHAR);
		statement.setNull(5, Types.VARCHAR,  "VARCHAR");
		statement.setNString(6, "sedan");
		statement.setArray(7, new FireboltArray(FireboltDataType.TEXT, new String[] {"sedan", "hatchback", "coupe"}));
		statement.execute();
		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(this.properties),
				anyInt(), anyInt(), anyBoolean(), any());

		assertEquals("INSERT INTO cars (sales, make, model, minor_model, color, type, types) VALUES (500,'Ford','FOCUS',NULL,NULL,'sedan',['sedan','hatchback','coupe'])",
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
		verify(fireboltStatementService, times(2)).execute(queryInfoWrapperArgumentCaptor.capture(),
				eq(this.properties), anyInt(), anyInt(), anyBoolean(), any());

		assertEquals("INSERT INTO cars (sales, make) VALUES (150,'Ford')",
				queryInfoWrapperArgumentCaptor.getAllValues().get(0).getSql());
		assertEquals("INSERT INTO cars (sales, make) VALUES (300,'Tesla')",
				queryInfoWrapperArgumentCaptor.getAllValues().get(1).getSql());
	}

	@Test
	void shouldThrowExceptionWhenAllParametersAreNotDefined() {
		FireboltPreparedStatement statementWithUndefinedParamWithSpace = FireboltPreparedStatement.statementBuilder()
				.sql("SELECT * FROM cars WHERE make LIKE ?").build();

		FireboltPreparedStatement statementWithUndefinedParamWithComma = FireboltPreparedStatement.statementBuilder()
				.sql("SELECT * FROM cars WHERE make LIKE ,?").build();

		FireboltPreparedStatement statementWithUndefinedParamWithParenthesis = FireboltPreparedStatement
				.statementBuilder().sql("SELECT * FROM cars WHERE make LIKE (?").build();

		assertThrows(IllegalArgumentException.class, statementWithUndefinedParamWithSpace::executeQuery);
		assertThrows(IllegalArgumentException.class, statementWithUndefinedParamWithComma::executeQuery);
		assertThrows(IllegalArgumentException.class, statementWithUndefinedParamWithParenthesis::executeQuery);
	}

	@Test
	void shouldExecuteWithSpecialCharactersInQuery() throws SQLException {
		String sql = "INSERT INTO cars (model ,sales, make) VALUES (?,?,'(?:^|[^\\\\p{L}\\\\p{N}])(?i)(phone)(?:[^\\\\p{L}\\\\p{N}]|$)')";
		statement = createStatementWithSql(sql);

		statement.setObject(1, "?");
		statement.setObject(2, " ?");

		String expectedSql = "INSERT INTO cars (model ,sales, make) VALUES ('?',' ?','(?:^|[^\\\\p{L}\\\\p{N}])(?i)(phone)(?:[^\\\\p{L}\\\\p{N}]|$)')";

		statement.execute();
		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(this.properties),
				anyInt(), anyInt(), anyBoolean(), any());
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

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(this.properties),
				anyInt(), anyInt(), anyBoolean(), any());
		assertEquals("INSERT INTO cars (sales, make) VALUES (NULL,NULL)",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	@Test
	void shouldSetBoolean() throws SQLException {
		statement = createStatementWithSql("INSERT INTO cars(available) VALUES (?)");

		statement.setBoolean(1, true);
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(this.properties),
				anyInt(), anyInt(), anyBoolean(), any());
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

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(this.properties),
				anyInt(), anyInt(), anyBoolean(), any());
	}

	@Test
	void shouldSetLong() throws SQLException {
		statement = createStatementWithSql("INSERT INTO cars(price) VALUES (?)");

		statement.setLong(1, 50L);
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(this.properties),
				anyInt(), anyInt(), anyBoolean(), any());
		assertEquals("INSERT INTO cars(price) VALUES (50)", queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	@Test
	void shouldSetFloat() throws SQLException {
		statement = createStatementWithSql("INSERT INTO cars(price) VALUES (?)");

		statement.setFloat(1, 5.5F);
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(this.properties),
				anyInt(), anyInt(), anyBoolean(), any());

		assertEquals("INSERT INTO cars(price) VALUES (5.5)", queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	@Test
	void shouldSetDouble() throws SQLException {
		statement = createStatementWithSql("INSERT INTO cars(price) VALUES (?)");

		statement.setDouble(1, 5.5);
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(this.properties),
				anyInt(), anyInt(), anyBoolean(), any());
	}

	@Test
	void shouldSetBigDecimal() throws SQLException {
		statement = createStatementWithSql("INSERT INTO cars(price) VALUES (?)");

		statement.setBigDecimal(1, new BigDecimal("555555555555.55555555"));
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(this.properties),
				anyInt(), anyInt(), anyBoolean(), any());
		assertEquals("INSERT INTO cars(price) VALUES (555555555555.55555555)",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	@Test
	@DefaultTimeZone("Europe/London")
	void shouldSetDate() throws SQLException {
		statement = createStatementWithSql("INSERT INTO cars(release_date) VALUES (?)");

		statement.setDate(1, new Date(1564527600000L));
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(this.properties),
				anyInt(), anyInt(), anyBoolean(), any());
		assertEquals("INSERT INTO cars(release_date) VALUES ('2019-07-31')",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	@Test
	@DefaultTimeZone("Europe/London")
	void shouldSetTimeStamp() throws SQLException {
		statement = createStatementWithSql("INSERT INTO cars(release_date) VALUES (?)");

		statement.setTimestamp(1, new Timestamp(1564571713000L));
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(this.properties),
				anyInt(), anyInt(), anyBoolean(), any());

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

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(this.properties),
				anyInt(), anyInt(), anyBoolean(), any());

		assertEquals(
				"INSERT INTO cars(timestamp, date, float, long, big_decimal, null, boolean, int) VALUES ('2019-07-31 12:15:13','2019-07-31',5.5,5,555555555555.55555555,NULL,1,5)",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	private FireboltPreparedStatement createStatementWithSql(String sql) {
		return FireboltPreparedStatement.statementBuilder().statementService(fireboltStatementService).sql(sql)
				.sessionProperties(properties).connection(connection).build();
	}
}
