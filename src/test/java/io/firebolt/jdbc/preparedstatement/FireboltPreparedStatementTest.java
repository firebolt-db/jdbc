package io.firebolt.jdbc.preparedstatement;

import io.firebolt.jdbc.connection.FireboltConnectionTokens;
import io.firebolt.jdbc.connection.settings.FireboltProperties;
import io.firebolt.jdbc.exception.FireboltException;
import io.firebolt.jdbc.service.FireboltQueryService;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Timestamp;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class FireboltPreparedStatementTest {

  @Mock private FireboltQueryService fireboltQueryService;

  @Mock private FireboltProperties properties;

  @Mock FireboltConnectionTokens connectionTokens;

  @BeforeAll
  public static void beforeAll(){
    TimeZone.setDefault(TimeZone.getTimeZone("Europe/London"));
  }

  @BeforeEach
  void beforeEach() throws FireboltException {
    lenient().when(connectionTokens.getAccessToken()).thenReturn("ACCESS");
    lenient()
        .when(fireboltQueryService.executeQuery(any(), any(), any(), any()))
        .thenReturn(Mockito.mock(InputStream.class));
  }

  @Test
  void shouldExecute() throws SQLException {
    FireboltPreparedStatement statement =
        FireboltPreparedStatement.statementBuilder()
            .fireboltQueryService(fireboltQueryService)
            .sql("INSERT INTO cars (sales, make) VALUES (?,?)")
            .connectionTokens(connectionTokens)
            .sessionProperties(properties)
            .build();

    statement.setObject(1, 500);
    statement.setObject(2, "Ford");
    statement.execute();
    verify(fireboltQueryService)
        .executeQuery(
            eq("INSERT INTO cars (sales, make) VALUES (500,'Ford')"),
            anyString(),
            anyString(),
            eq(this.properties));
  }

  @Test
  void shouldExecuteBatch() throws SQLException {
    FireboltPreparedStatement statement =
        FireboltPreparedStatement.statementBuilder()
            .fireboltQueryService(fireboltQueryService)
            .sql("INSERT INTO cars (sales, make) VALUES (?,?)")
            .connectionTokens(connectionTokens)
            .sessionProperties(properties)
            .build();

    statement.setObject(1, 150);
    statement.setObject(2, "Ford");
    statement.addBatch();
    statement.setObject(1, 300);
    statement.setObject(2, "Tesla");
    statement.addBatch();
    statement.executeBatch();
    verify(fireboltQueryService)
        .executeQuery(
            eq("INSERT INTO cars (sales, make) VALUES (150,'Ford')"),
            anyString(),
            anyString(),
            eq(this.properties));

    verify(fireboltQueryService)
        .executeQuery(
            eq("INSERT INTO cars (sales, make) VALUES (300,'Tesla')"),
            anyString(),
            anyString(),
            eq(this.properties));
  }

  @Test
  void shouldThrowsExceptionWhenAllParametersAreNotDefined() {
    FireboltPreparedStatement statementWithUndefinedParamWithSpace =
        FireboltPreparedStatement.statementBuilder()
            .sql("SELECT * FROM cars WHERE make LIKE ?")
            .build();

    FireboltPreparedStatement statementWithUndefinedParamWithComma =
        FireboltPreparedStatement.statementBuilder()
            .sql("SELECT * FROM cars WHERE make LIKE ,?")
            .build();

    FireboltPreparedStatement statementWithUndefinedParamWithParenthesis =
        FireboltPreparedStatement.statementBuilder()
            .sql("SELECT * FROM cars WHERE make LIKE (?")
            .build();

    assertThrows(
        IllegalArgumentException.class, statementWithUndefinedParamWithSpace::executeQuery);
    assertThrows(
        IllegalArgumentException.class, statementWithUndefinedParamWithComma::executeQuery);
    assertThrows(
        IllegalArgumentException.class, statementWithUndefinedParamWithParenthesis::executeQuery);
  }

  @Test
  void shouldThrowsExceptionWhenTryingToExecuteUpdate() throws SQLException {
    FireboltPreparedStatement statement =
        FireboltPreparedStatement.statementBuilder()
            .sql("update cars set sales = ? where make = ?")
            .build();

    statement.setObject(1, 150);
    statement.setObject(2, "Ford");

    assertThrows(SQLFeatureNotSupportedException.class, statement::executeUpdate);
  }

  @Test
  void shouldSetNull() throws SQLException {
    FireboltPreparedStatement statement =
        createStatementWithSql("INSERT INTO cars (sales, make) VALUES (?,?)");

    statement.setNull(1, 0);
    statement.setNull(2, 0);
    statement.execute();

    verify(fireboltQueryService)
        .executeQuery(
            eq("INSERT INTO cars (sales, make) VALUES (\\N,\\N)"),
            anyString(),
            anyString(),
            eq(this.properties));
  }

  @Test
  void shouldSetBoolean() throws SQLException {
    FireboltPreparedStatement statement =
        createStatementWithSql("INSERT INTO cars(available) VALUES (?)");

    statement.setBoolean(1, true);
    statement.execute();

    verify(fireboltQueryService)
        .executeQuery(
            eq("INSERT INTO cars(available) VALUES (1)"), // check 1 or true ?!
            anyString(),
            anyString(),
            eq(this.properties));
  }

  @Test
  void shouldThrowExceptionWhenTryingToSetByte() throws SQLException {
    FireboltPreparedStatement statement =
        createStatementWithSql("INSERT INTO cars(make) VALUES (?)");
    assertThrows(SQLFeatureNotSupportedException.class, () -> statement.setByte(1, (byte) 127));
  }

  @Test
  void shouldThrowExceptionWhenTryingToSetBytes() throws SQLException {
    FireboltPreparedStatement statement =
        createStatementWithSql("INSERT INTO cars(make) VALUES (?)");
    assertThrows(
        SQLFeatureNotSupportedException.class, () -> statement.setBytes(1, "bytes".getBytes()));
  }

  @Test
  void shouldSetInt() throws SQLException {
    FireboltPreparedStatement statement =
        createStatementWithSql("INSERT INTO cars(price) VALUES (?)");

    statement.setInt(1, 50);
    statement.execute();

    verify(fireboltQueryService)
        .executeQuery(
            eq("INSERT INTO cars(price) VALUES (50)"),
            anyString(),
            anyString(),
            eq(this.properties));
  }

  @Test
  void shouldSetLong() throws SQLException {
    FireboltPreparedStatement statement =
        createStatementWithSql("INSERT INTO cars(price) VALUES (?)");

    statement.setLong(1, 50L);
    statement.execute();

    verify(fireboltQueryService)
        .executeQuery(
            eq("INSERT INTO cars(price) VALUES (50)"),
            anyString(),
            anyString(),
            eq(this.properties));
  }

  @Test
  void shouldSetFloat() throws SQLException {
    FireboltPreparedStatement statement =
        createStatementWithSql("INSERT INTO cars(price) VALUES (?)");

    statement.setFloat(1, 5.5F);
    statement.execute();

    verify(fireboltQueryService)
        .executeQuery(
            eq("INSERT INTO cars(price) VALUES (5.5)"),
            anyString(),
            anyString(),
            eq(this.properties));
  }

  @Test
  void shouldSetDouble() throws SQLException {
    FireboltPreparedStatement statement =
        createStatementWithSql("INSERT INTO cars(price) VALUES (?)");

    statement.setDouble(1, 5.5);
    statement.execute();

    verify(fireboltQueryService)
        .executeQuery(
            eq("INSERT INTO cars(price) VALUES (5.5)"),
            anyString(),
            anyString(),
            eq(this.properties));
  }

  @Test
  void shouldSetBigDecimal() throws SQLException {
    FireboltPreparedStatement statement =
        createStatementWithSql("INSERT INTO cars(price) VALUES (?)");

    statement.setBigDecimal(1, new BigDecimal("555555555555.55555555"));
    statement.execute();

    verify(fireboltQueryService)
        .executeQuery(
            eq("INSERT INTO cars(price) VALUES (555555555555.55555555)"),
            anyString(),
            anyString(),
            eq(this.properties));
  }

  @Test
  void shouldSetDate() throws SQLException {
    FireboltPreparedStatement statement =
        createStatementWithSql("INSERT INTO cars(release_date) VALUES (?)");

    statement.setDate(1, new Date(1564527600000L));
    statement.execute();

    verify(fireboltQueryService)
        .executeQuery(
            eq("INSERT INTO cars(release_date) VALUES ('2019-07-31')"),
            anyString(),
            anyString(),
            eq(this.properties));
  }

  @Test
  void shouldSetTimeStamp() throws SQLException {
    FireboltPreparedStatement statement =
        createStatementWithSql("INSERT INTO cars(release_date) VALUES (?)");

    statement.setTimestamp(1, new Timestamp(1564571713000L));
    statement.execute();

    verify(fireboltQueryService)
        .executeQuery(
            eq("INSERT INTO cars(release_date) VALUES ('2019-07-31 12:15:13')"),
            anyString(),
            anyString(),
            eq(this.properties));
  }

  @Test
  void shouldSetAllObjects() throws SQLException {
    FireboltPreparedStatement statement =
        createStatementWithSql(
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

    verify(fireboltQueryService)
        .executeQuery(
            eq(
                "INSERT INTO cars(timestamp, date, float, long, big_decimal, null, boolean, int) VALUES ('2019-07-31 12:15:13','2019-07-31',5.5,5,555555555555.55555555,\\N,1,5)"),
            anyString(),
            anyString(),
            eq(this.properties));
  }
  
  @Test
  void shouldThrowExceptionWhenTryingToAddBatchForSelectQuery() throws SQLException {
    FireboltPreparedStatement statement =
        createStatementWithSql("SELECT * FROM employees WHERE id = ?");

    statement.setObject(1, 1);
    assertThrows(FireboltException.class, statement::addBatch);
  }

  private FireboltPreparedStatement createStatementWithSql(String sql) {
    return FireboltPreparedStatement.statementBuilder()
        .fireboltQueryService(fireboltQueryService)
        .sql(sql)
        .connectionTokens(connectionTokens)
        .sessionProperties(properties)
        .build();
  }
}
