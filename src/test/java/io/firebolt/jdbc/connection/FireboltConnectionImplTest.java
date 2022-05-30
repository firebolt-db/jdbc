package io.firebolt.jdbc.connection;

import io.firebolt.jdbc.exception.FireboltException;
import io.firebolt.jdbc.service.FireboltAuthenticationService;
import io.firebolt.jdbc.service.FireboltEngineService;
import io.firebolt.jdbc.service.FireboltQueryService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FireboltConnectionImplTest {

  private final FireboltConnectionTokens fireboltConnectionTokens =
      FireboltConnectionTokens.builder().build();
  @Mock private FireboltAuthenticationService fireboltAuthenticationService;
  @Mock private FireboltEngineService fireboltEngineService;

  @Mock private FireboltQueryService fireboltQueryService;
  private Properties connectionProperties;

  private static final String URL = "jdbc:firebolt://firebolt.io/db";

  @BeforeEach
  void init() {
    connectionProperties = new Properties();
    connectionProperties.put("user", "user");
    connectionProperties.put("password", "pa$$word");
    connectionProperties.put("host", "firebolt.io");
    when(fireboltAuthenticationService.getConnectionTokens(
            "https://firebolt.io", "user", "pa$$word"))
        .thenReturn(fireboltConnectionTokens);
  }

  @Test
  void shouldInitConnection() throws FireboltException {
    FireboltConnectionImpl fireboltConnectionImpl =
        new FireboltConnectionImpl(
                URL,
                connectionProperties,
                fireboltAuthenticationService,
                fireboltEngineService,
                fireboltQueryService)
            .connect();
    verify(fireboltAuthenticationService)
        .getConnectionTokens("https://firebolt.io", "user", "pa$$word");
    assertNotNull(fireboltConnectionImpl);
  }

  @Test
  void shouldPrepareStatement() throws SQLException, IOException {
    when(fireboltQueryService.executeQuery(any(), any(), any(), any()))
        .thenReturn(new ByteArrayInputStream("".getBytes()));
    FireboltConnectionImpl fireboltConnectionImpl =
        new FireboltConnectionImpl(
                URL,
                connectionProperties,
                fireboltAuthenticationService,
                fireboltEngineService,
                fireboltQueryService)
            .connect();
    PreparedStatement statement =
        fireboltConnectionImpl.prepareStatement("INSERT INTO cars(sales, name) VALUES (?, ?)");
    statement.setObject(1, 500);
    statement.setObject(2, "Ford");
    statement.execute();
    assertNotNull(fireboltConnectionImpl);
    assertNotNull(statement);
    verify(fireboltQueryService)
        .executeQuery(
            eq("INSERT INTO cars(sales, name) VALUES (500, 'Ford')"), any(), any(), any());
  }

  @Test
  void shouldCloseAllStatementsOnClose() throws SQLException {
    FireboltConnectionImpl fireboltConnectionImpl =
            new FireboltConnectionImpl(
                    URL,
                    connectionProperties,
                    fireboltAuthenticationService,
                    fireboltEngineService,
                    fireboltQueryService)
                    .connect();
    Statement statement = fireboltConnectionImpl.createStatement();
    Statement preparedStatement = fireboltConnectionImpl.prepareStatement("test");
    fireboltConnectionImpl.close();
    assertTrue(statement.isClosed());
    assertTrue(preparedStatement.isClosed());
    assertTrue(fireboltConnectionImpl.isClosed());
  }
}
