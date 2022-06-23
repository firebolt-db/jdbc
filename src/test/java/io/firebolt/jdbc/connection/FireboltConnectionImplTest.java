package io.firebolt.jdbc.connection;

import io.firebolt.jdbc.connection.settings.FireboltProperties;
import io.firebolt.jdbc.exception.FireboltException;
import io.firebolt.jdbc.service.FireboltAuthenticationService;
import io.firebolt.jdbc.service.FireboltEngineService;
import io.firebolt.jdbc.service.FireboltQueryService;
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

  @Captor ArgumentCaptor<FireboltProperties> propertiesArgumentCaptor;

  private Properties connectionProperties = new Properties();

  private static final String URL = "jdbc:firebolt://api.dev.firebolt.io/db";
  private static final String LOCAL_URL =
      "jdbc:firebolt://localhost:8123/local_dev_db?ssl=false&max_query_size=10000000&use_standard_sql=1&mask_internal_errors=0&firebolt_enable_beta_functions=1&firebolt_case_insensitive_identifiers=1&rest_api_pull_timeout_sec=3600&rest_api_pull_interval_millisec=5000&rest_api_retry_times=10";

  @BeforeEach
  void init() {
    connectionProperties = new Properties();
    connectionProperties.put("user", "user");
    connectionProperties.put("password", "pa$$word");
    lenient()
        .when(
            fireboltAuthenticationService.getConnectionTokens(
                "https://api.dev.firebolt.io:443", "user", "pa$$word"))
        .thenReturn(fireboltConnectionTokens);
  }

  @Test
  void shouldInitConnection() throws SQLException {
    FireboltConnectionImpl fireboltConnectionImpl =
        new FireboltConnectionImpl(
            URL,
            connectionProperties,
            fireboltAuthenticationService,
            fireboltEngineService,
            fireboltQueryService);
    verify(fireboltAuthenticationService)
        .getConnectionTokens("https://api.dev.firebolt.io:443", "user", "pa$$word");
    assertFalse(fireboltConnectionImpl.isClosed());
  }

  @Test
  void shouldNotFetchTokenNorEngineHostForLocalFirebolt() throws SQLException {
    FireboltConnectionImpl fireboltConnectionImpl =
        new FireboltConnectionImpl(
            LOCAL_URL,
            connectionProperties,
            fireboltAuthenticationService,
            fireboltEngineService,
            fireboltQueryService);
    verifyNoInteractions(fireboltAuthenticationService);
    verifyNoInteractions(fireboltEngineService);
    assertFalse(fireboltConnectionImpl.isClosed());
  }

  @Test
  void shouldPrepareStatement() throws SQLException, IOException {
    when(fireboltQueryService.executeQuery(any(), anyBoolean(), any(), any(), any()))
        .thenReturn(new ByteArrayInputStream("".getBytes()));
    FireboltConnectionImpl fireboltConnectionImpl =
        new FireboltConnectionImpl(
            URL,
            connectionProperties,
            fireboltAuthenticationService,
            fireboltEngineService,
            fireboltQueryService);
    PreparedStatement statement =
        fireboltConnectionImpl.prepareStatement("INSERT INTO cars(sales, name) VALUES (?, ?)");
    statement.setObject(1, 500);
    statement.setObject(2, "Ford");
    statement.execute();
    assertNotNull(fireboltConnectionImpl);
    assertNotNull(statement);
    verify(fireboltQueryService)
        .executeQuery(
            eq("INSERT INTO cars(sales, name) VALUES (500, 'Ford')"),
            eq(false),
            any(),
            any(),
            any());
  }

  @Test
  void shouldCloseAllStatementsOnClose() throws SQLException {
    FireboltConnectionImpl fireboltConnectionImpl =
        new FireboltConnectionImpl(
            URL,
            connectionProperties,
            fireboltAuthenticationService,
            fireboltEngineService,
            fireboltQueryService);
    Statement statement = fireboltConnectionImpl.createStatement();
    Statement preparedStatement = fireboltConnectionImpl.prepareStatement("test");
    fireboltConnectionImpl.close();
    assertTrue(statement.isClosed());
    assertTrue(preparedStatement.isClosed());
    assertTrue(fireboltConnectionImpl.isClosed());
  }

  @Test
  void shouldNotSetNewPropertyWhenWhenConnectionIsNotValidWithTheNewProperty() throws SQLException {
    FireboltConnectionImpl fireboltConnectionImpl =
        new FireboltConnectionImpl(
            URL,
            connectionProperties,
            fireboltAuthenticationService,
            fireboltEngineService,
            fireboltQueryService);

    when(fireboltQueryService.executeQuery(any(), anyBoolean(), any(), any(), any()))
        .thenThrow(new IllegalArgumentException("The property is invalid"));
    assertThrows(
        FireboltException.class,
        () -> fireboltConnectionImpl.addProperty(new ImmutablePair<>("custom_1", "1")));

    verify(fireboltQueryService)
        .executeQuery(
            eq("SELECT 1"), anyBoolean(), any(), any(), propertiesArgumentCaptor.capture());
    assertEquals(
        "1", propertiesArgumentCaptor.getValue().getAdditionalProperties().get("custom_1"));

    assertNull(
        fireboltConnectionImpl.getSessionProperties().getAdditionalProperties().get("custom_1"));
  }

  @Test
  void shouldSetNewPropertyWhenConnectionIsValidWithTheNewProperty() throws SQLException {
    when(fireboltQueryService.executeQuery(any(), anyBoolean(), any(), any(), any()))
        .thenReturn(new ByteArrayInputStream("".getBytes()));
    FireboltConnectionImpl fireboltConnectionImpl =
        new FireboltConnectionImpl(
            URL,
            connectionProperties,
            fireboltAuthenticationService,
            fireboltEngineService,
            fireboltQueryService);

    Pair<String, String> newProperties = new ImmutablePair<>("custom_1", "1");

    fireboltConnectionImpl.addProperty(newProperties);

    verify(fireboltQueryService)
        .executeQuery(
            eq("SELECT 1"), anyBoolean(), any(), any(), propertiesArgumentCaptor.capture());
    assertEquals(
        "1", propertiesArgumentCaptor.getValue().getAdditionalProperties().get("custom_1"));
    assertEquals(
        "1",
        fireboltConnectionImpl.getSessionProperties().getAdditionalProperties().get("custom_1"));
  }
}
