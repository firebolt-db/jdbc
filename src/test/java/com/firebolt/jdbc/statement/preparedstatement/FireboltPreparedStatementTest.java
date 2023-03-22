package com.firebolt.jdbc.statement.preparedstatement;

import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.service.FireboltStatementService;
import com.firebolt.jdbc.statement.StatementInfoWrapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junitpioneer.jupiter.DefaultTimeZone;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Timestamp;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.lenient;
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

	@BeforeEach
	void beforeEach() throws SQLException {
		lenient().when(properties.getBufferSize()).thenReturn(10);
		lenient().when(fireboltStatementService.execute(any(), any(), anyInt(), anyInt(), anyBoolean(), anyBoolean(), any()))
				.thenReturn(Optional.empty());
	}

	@Test
	void shouldExecute() throws SQLException {
		FireboltPreparedStatement statement = FireboltPreparedStatement.statementBuilder()
				.statementService(fireboltStatementService).sql("INSERT INTO cars (sales, make) VALUES (?,?)")
				.sessionProperties(properties).build();

		statement.setObject(1, 500);
		statement.setObject(2, "Ford");
		statement.execute();
		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(this.properties),
				anyInt(), anyInt(), anyBoolean(), anyBoolean(), any());

		assertEquals("INSERT INTO cars (sales, make) VALUES (500,'Ford')",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	@Test
	void shouldExecuteBatch() throws SQLException {
		FireboltPreparedStatement statement = FireboltPreparedStatement.statementBuilder()
				.statementService(fireboltStatementService).sql("INSERT INTO cars (sales, make) VALUES (?,?)")
				.sessionProperties(properties).build();

		statement.setObject(1, 150);
		statement.setObject(2, "Ford");
		statement.addBatch();
		statement.setObject(1, 300);
		statement.setObject(2, "Tesla");
		statement.addBatch();
		statement.executeBatch();
		verify(fireboltStatementService, times(2)).execute(queryInfoWrapperArgumentCaptor.capture(),
				eq(this.properties), anyInt(), anyInt(), anyBoolean(), anyBoolean(), any());

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
		FireboltPreparedStatement statement = FireboltPreparedStatement.statementBuilder()
				.statementService(fireboltStatementService).sql(sql).sessionProperties(properties).build();

		statement.setObject(1, "?");
		statement.setObject(2, " ?");

		String expectedSql = "INSERT INTO cars (model ,sales, make) VALUES ('?',' ?','(?:^|[^\\\\p{L}\\\\p{N}])(?i)(phone)(?:[^\\\\p{L}\\\\p{N}]|$)')";

		statement.execute();
		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(this.properties),
				anyInt(), anyInt(), anyBoolean(), anyBoolean(), any());
		assertEquals(expectedSql, queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	@Test
	void shouldThrowExceptionWhenTooManyParametersAreProvided() throws SQLException {
		String sql = "INSERT INTO cars (model ,sales, make) VALUES (?, '(?:^|[^\\\\p{L}\\\\p{N}])(?i)(phone)(?:[^\\\\p{L}\\\\p{N}]|$)')";
		FireboltPreparedStatement statement = FireboltPreparedStatement.statementBuilder()
				.statementService(fireboltStatementService).sql(sql).sessionProperties(properties).build();

		statement.setObject(1, "A");

		assertThrows(FireboltException.class, () -> statement.setObject(2, "B"));
	}

	@Test
	void shouldThrowsExceptionWhenTryingToExecuteUpdate() throws SQLException {
		FireboltPreparedStatement statement = FireboltPreparedStatement.statementBuilder()
				.sql("update cars set sales = ? where make = ?").build();

		statement.setObject(1, 150);
		statement.setObject(2, "Ford");

		assertThrows(FireboltException.class,
				() -> statement.executeUpdate("update cars set sales = 50 where make = 'Ford"));
	}

	@Test
	void shouldSetNull() throws SQLException {
		FireboltPreparedStatement statement = createStatementWithSql("INSERT INTO cars (sales, make) VALUES (?,?)");

		statement.setNull(1, 0);
		statement.setNull(2, 0);
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(this.properties),
				anyInt(), anyInt(), anyBoolean(), anyBoolean(), any());
		assertEquals("INSERT INTO cars (sales, make) VALUES (NULL,NULL)",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	@Test
	void shouldSetBoolean() throws SQLException {
		FireboltPreparedStatement statement = createStatementWithSql("INSERT INTO cars(available) VALUES (?)");

		statement.setBoolean(1, true);
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(this.properties),
				anyInt(), anyInt(), anyBoolean(), anyBoolean(), any());
		assertEquals("INSERT INTO cars(available) VALUES (1)", queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	@Test
	void shouldThrowExceptionWhenTryingToSetByte() throws SQLException {
		FireboltPreparedStatement statement = createStatementWithSql("INSERT INTO cars(make) VALUES (?)");
		assertThrows(SQLFeatureNotSupportedException.class, () -> statement.setByte(1, (byte) 127));
	}

	@Test
	void shouldThrowExceptionWhenTryingToSetBytes() throws SQLException {
		FireboltPreparedStatement statement = createStatementWithSql("INSERT INTO cars(make) VALUES (?)");
		assertThrows(SQLFeatureNotSupportedException.class, () -> statement.setBytes(1, "bytes".getBytes()));
	}

	@Test
	void shouldSetInt() throws SQLException {
		FireboltPreparedStatement statement = createStatementWithSql("INSERT INTO cars(price) VALUES (?)");

		statement.setInt(1, 50);
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(this.properties),
				anyInt(), anyInt(), anyBoolean(), anyBoolean(), any());
	}

	@Test
	void shouldSetLong() throws SQLException {
		FireboltPreparedStatement statement = createStatementWithSql("INSERT INTO cars(price) VALUES (?)");

		statement.setLong(1, 50L);
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(this.properties),
				anyInt(), anyInt(), anyBoolean(), anyBoolean(), any());
		assertEquals("INSERT INTO cars(price) VALUES (50)", queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	@Test
	void shouldSetFloat() throws SQLException {
		FireboltPreparedStatement statement = createStatementWithSql("INSERT INTO cars(price) VALUES (?)");

		statement.setFloat(1, 5.5F);
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(this.properties),
				anyInt(), anyInt(), anyBoolean(), anyBoolean(), any());

		assertEquals("INSERT INTO cars(price) VALUES (5.5)", queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	@Test
	void shouldSetDouble() throws SQLException {
		FireboltPreparedStatement statement = createStatementWithSql("INSERT INTO cars(price) VALUES (?)");

		statement.setDouble(1, 5.5);
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(this.properties),
				anyInt(), anyInt(), anyBoolean(), anyBoolean(), any());
	}

	@Test
	void shouldSetBigDecimal() throws SQLException {
		FireboltPreparedStatement statement = createStatementWithSql("INSERT INTO cars(price) VALUES (?)");

		statement.setBigDecimal(1, new BigDecimal("555555555555.55555555"));
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(this.properties),
				anyInt(), anyInt(), anyBoolean(), anyBoolean(), any());
		assertEquals("INSERT INTO cars(price) VALUES (555555555555.55555555)",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	@Test
	@DefaultTimeZone("Europe/London")
	void shouldSetDate() throws SQLException {
		FireboltPreparedStatement statement = createStatementWithSql("INSERT INTO cars(release_date) VALUES (?)");

		statement.setDate(1, new Date(1564527600000L));
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(this.properties),
				anyInt(), anyInt(), anyBoolean(), anyBoolean(), any());
		assertEquals("INSERT INTO cars(release_date) VALUES ('2019-07-31')",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	@Test
	@DefaultTimeZone("Europe/London")
	void shouldSetTimeStamp() throws SQLException {
		FireboltPreparedStatement statement = createStatementWithSql("INSERT INTO cars(release_date) VALUES (?)");

		statement.setTimestamp(1, new Timestamp(1564571713000L));
		statement.execute();

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(this.properties),
				anyInt(), anyInt(), anyBoolean(), anyBoolean(), any());

		assertEquals("INSERT INTO cars(release_date) VALUES ('2019-07-31 12:15:13')",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	@Test
	@DefaultTimeZone("Europe/London")
	void shouldSetAllObjects() throws SQLException {
		FireboltPreparedStatement statement = createStatementWithSql(
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
				anyInt(), anyInt(), anyBoolean(), anyBoolean(), any());

		assertEquals(
				"INSERT INTO cars(timestamp, date, float, long, big_decimal, null, boolean, int) VALUES ('2019-07-31 12:15:13','2019-07-31',5.5,5,555555555555.55555555,NULL,1,5)",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	private FireboltPreparedStatement createStatementWithSql(String sql) {
		return FireboltPreparedStatement.statementBuilder().statementService(fireboltStatementService).sql(sql)
				.sessionProperties(properties).build();
	}
}
