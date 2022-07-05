package io.firebolt.jdbc.connection;

import io.firebolt.jdbc.connection.settings.FireboltProperties;
import io.firebolt.jdbc.exception.ExceptionType;
import io.firebolt.jdbc.exception.FireboltException;
import io.firebolt.jdbc.service.FireboltAuthenticationService;
import io.firebolt.jdbc.service.FireboltEngineService;
import io.firebolt.jdbc.service.FireboltStatementService;
import io.firebolt.jdbc.statement.StatementInfoWrapper;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FireboltConnectionTest {

  private final FireboltConnectionTokens fireboltConnectionTokens =
      FireboltConnectionTokens.builder().build();
  @Mock private FireboltAuthenticationService fireboltAuthenticationService;
  @Mock private FireboltEngineService fireboltEngineService;

  @Mock private FireboltStatementService fireboltStatementService;

  @Captor ArgumentCaptor<FireboltProperties> propertiesArgumentCaptor;
  @Captor ArgumentCaptor<StatementInfoWrapper> queryInfoWrapperArgumentCaptor;

  private Properties connectionProperties = new Properties();

  private static final String URL = "jdbc:firebolt://api.dev.firebolt.io/db";
  private static final String LOCAL_URL =
      "jdbc:firebolt://localhost:8123/local_dev_db?ssl=false&max_query_size=10000000&use_standard_sql=1&mask_internal_errors=0&firebolt_enable_beta_functions=1&firebolt_case_insensitive_identifiers=1&rest_api_pull_timeout_sec=3600&rest_api_pull_interval_millisec=5000&rest_api_retry_times=10";

  @BeforeEach
  void init() throws FireboltException {
    connectionProperties = new Properties();
    connectionProperties.put("user", "user");
    connectionProperties.put("password", "pa$$word");
    connectionProperties.put("compress", "1");
    lenient()
        .when(
            fireboltAuthenticationService.getConnectionTokens(
                eq("https://api.dev.firebolt.io:443"), any()))
        .thenReturn(fireboltConnectionTokens);
  }

  @Test
  void shouldInitConnection() throws SQLException {
    FireboltConnection fireboltConnection =
        new FireboltConnection(
            URL,
            connectionProperties,
            fireboltAuthenticationService,
            fireboltEngineService,
            fireboltStatementService);
    assertFalse(fireboltConnection.isClosed());
  }

  @Test
  void shouldReInitConnectionWhenTokenIsExpired() throws SQLException {
    when(fireboltEngineService.getEngineHost(any(), any()))
        .thenThrow(new FireboltException("The token is expired", ExceptionType.EXPIRED_TOKEN))
        .thenReturn("engine");
    FireboltConnection fireboltConnection =
        new FireboltConnection(
            URL,
            connectionProperties,
            fireboltAuthenticationService,
            fireboltEngineService,
            fireboltStatementService);
    verify(fireboltEngineService, times(2)).getEngineHost(any(), any());
    assertFalse(fireboltConnection.isClosed());
  }

  @Test
  void shouldNotFetchTokenNorEngineHostForLocalFirebolt() throws SQLException {
    FireboltConnection fireboltConnection =
        new FireboltConnection(
            LOCAL_URL,
            connectionProperties,
            fireboltAuthenticationService,
            fireboltEngineService,
            fireboltStatementService);
    verifyNoInteractions(fireboltAuthenticationService);
    verifyNoInteractions(fireboltEngineService);
    assertFalse(fireboltConnection.isClosed());
  }

  @Test
  void shouldPrepareStatement() throws SQLException {
    when(fireboltStatementService.execute(any(), any(), any()))
        .thenReturn(new ByteArrayInputStream("".getBytes()));
    FireboltConnection fireboltConnection =
        new FireboltConnection(
            URL,
            connectionProperties,
            fireboltAuthenticationService,
            fireboltEngineService,
            fireboltStatementService);
    PreparedStatement statement =
        fireboltConnection.prepareStatement("INSERT INTO cars(sales, name) VALUES (?, ?)");
    statement.setObject(1, 500);
    statement.setObject(2, "Ford");
    statement.execute();
    assertNotNull(fireboltConnection);
    assertNotNull(statement);
    verify(fireboltStatementService)
        .execute(queryInfoWrapperArgumentCaptor.capture(), any(), any());
    assertEquals(
        "INSERT INTO cars(sales, name) VALUES (500, 'Ford')",
        queryInfoWrapperArgumentCaptor.getValue().getSql());
  }

  @Test
  void shouldCloseAllStatementsOnClose() throws SQLException {
    FireboltConnection fireboltConnection =
        new FireboltConnection(
            URL,
            connectionProperties,
            fireboltAuthenticationService,
            fireboltEngineService,
            fireboltStatementService);
    Statement statement = fireboltConnection.createStatement();
    Statement preparedStatement = fireboltConnection.prepareStatement("test");
    fireboltConnection.close();
    assertTrue(statement.isClosed());
    assertTrue(preparedStatement.isClosed());
    assertTrue(fireboltConnection.isClosed());
  }

  @Test
  void shouldNotSetNewPropertyWhenWhenConnectionIsNotValidWithTheNewProperty() throws SQLException {
    FireboltConnection fireboltConnection =
        new FireboltConnection(
            URL,
            connectionProperties,
            fireboltAuthenticationService,
            fireboltEngineService,
            fireboltStatementService);

    when(fireboltStatementService.execute(any(), any(), any()))
        .thenThrow(new IllegalArgumentException("The property is invalid"));
    assertThrows(
        FireboltException.class,
        () -> fireboltConnection.addProperty(new ImmutablePair<>("custom_1", "1")));

    verify(fireboltStatementService)
        .execute(
            queryInfoWrapperArgumentCaptor.capture(), propertiesArgumentCaptor.capture(), any());
    assertEquals(
        "1", propertiesArgumentCaptor.getValue().getAdditionalProperties().get("custom_1"));
    assertEquals("SELECT 1", queryInfoWrapperArgumentCaptor.getValue().getSql());
    assertNull(fireboltConnection.getSessionProperties().getAdditionalProperties().get("custom_1"));
  }

  @Test
  void shouldSetNewPropertyWhenConnectionIsValidWithTheNewProperty() throws SQLException {
    when(fireboltStatementService.execute(any(), any(), any()))
        .thenReturn(new ByteArrayInputStream("".getBytes()));
    FireboltConnection fireboltConnection =
        new FireboltConnection(
            URL,
            connectionProperties,
            fireboltAuthenticationService,
            fireboltEngineService,
            fireboltStatementService);

    Pair<String, String> newProperties = new ImmutablePair<>("custom_1", "1");

    fireboltConnection.addProperty(newProperties);

    verify(fireboltStatementService)
        .execute(
            queryInfoWrapperArgumentCaptor.capture(), propertiesArgumentCaptor.capture(), any());
    assertEquals(
        "1", propertiesArgumentCaptor.getValue().getAdditionalProperties().get("custom_1"));
    assertEquals(
        "1", fireboltConnection.getSessionProperties().getAdditionalProperties().get("custom_1"));
    assertEquals("SELECT 1", queryInfoWrapperArgumentCaptor.getValue().getSql());
  }

  @Test
  void shouldExtractConnectorOverrides() throws SQLException {
    when(fireboltStatementService.execute(any(), any(), any()))
        .thenReturn(new ByteArrayInputStream("".getBytes()));
    connectionProperties.put("connector_versions", "ConnA:1.0.9,ConnB:2.8.0");

    FireboltConnection fireboltConnectionImpl =
        new FireboltConnection(
            URL,
            connectionProperties,
            fireboltAuthenticationService,
            fireboltEngineService,
            fireboltStatementService);

    PreparedStatement statement = fireboltConnectionImpl.prepareStatement("SELECT 1");
    statement.execute();

    verify(fireboltStatementService).execute(any(), propertiesArgumentCaptor.capture(), any());
    assertNull(
        propertiesArgumentCaptor.getValue().getAdditionalProperties().get("connector_versions"));
    assertNull(
        fireboltConnectionImpl
            .getSessionProperties()
            .getAdditionalProperties()
            .get("connector_versions"));
  }
}
