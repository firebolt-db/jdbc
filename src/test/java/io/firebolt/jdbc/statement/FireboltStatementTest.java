package io.firebolt.jdbc.statement;

import io.firebolt.jdbc.connection.FireboltConnection;
import io.firebolt.jdbc.connection.settings.FireboltProperties;
import io.firebolt.jdbc.resultset.FireboltResultSet;
import io.firebolt.jdbc.service.FireboltStatementService;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FireboltStatementTest {

  @Mock private FireboltStatementService fireboltStatementService;

  @Mock private FireboltConnection fireboltConnection;

  @Captor ArgumentCaptor<FireboltProperties> fireboltPropertiesArgumentCaptor;

  @Captor ArgumentCaptor<StatementInfoWrapper> queryInfoWrapperArgumentCaptor;

  @Captor ArgumentCaptor<Map<String, String>> mapArgumentCaptor;

  @Test
  void shouldExecuteQueryAndCreateResultSet() throws SQLException {
    try (MockedConstruction<FireboltResultSet> mocked =
        Mockito.mockConstruction(FireboltResultSet.class)) {
      FireboltProperties fireboltProperties =
          FireboltProperties.builder().additionalProperties(new HashMap<>()).build();
      FireboltStatement fireboltStatement =
          FireboltStatement.builder()
              .statementService(fireboltStatementService)
              .sessionProperties(fireboltProperties)
              .build();

      when(fireboltStatementService.execute(any(), any(), any()))
          .thenReturn(mock(InputStream.class));
      fireboltStatement.executeQuery("show database");
      assertTrue(fireboltProperties.getAdditionalProperties().isEmpty());
      verify(fireboltStatementService)
          .execute(queryInfoWrapperArgumentCaptor.capture(), eq(fireboltProperties), any());
      assertEquals(1, mocked.constructed().size());
      assertEquals(-1, fireboltStatement.getUpdateCount());
      assertEquals("show database", queryInfoWrapperArgumentCaptor.getValue().getSql());
      assertTrue(queryInfoWrapperArgumentCaptor.getValue().isQuery());
    }
  }

  @Test
  void shouldExtractAdditionalProperties() throws SQLException {
    try (MockedConstruction<FireboltResultSet> mockedResultSet =
        Mockito.mockConstruction(FireboltResultSet.class)) {
      FireboltConnection connection = mock(FireboltConnection.class);
      FireboltProperties fireboltProperties =
          FireboltProperties.builder().additionalProperties(new HashMap<>()).build();

      FireboltStatement fireboltStatement =
          FireboltStatement.builder()
              .statementService(fireboltStatementService)
              .sessionProperties(fireboltProperties)
              .connection(connection)
              .build();

      fireboltStatement.executeQuery("set custom_1 = 1");
      verifyNoMoreInteractions(fireboltStatementService);
      verify(connection).addProperty(new ImmutablePair<>("custom_1", "1"));
      assertEquals(0, mockedResultSet.constructed().size());
    }
  }

  @Test
  void shouldCancelByQueryWhenAggressiveCancelIsEnabled() throws SQLException, IOException {
    try (MockedConstruction<FireboltResultSet> mockedResultSet =
        Mockito.mockConstruction(FireboltResultSet.class)) {

      FireboltProperties fireboltProperties =
          FireboltProperties.builder()
              .database("db")
              .aggressiveCancel(true)
              .additionalProperties(new HashMap<>())
              .build();

      FireboltStatement fireboltStatement =
          FireboltStatement.builder()
              .statementService(fireboltStatementService)
              .sessionProperties(fireboltProperties)
              .connection(fireboltConnection)
              .build();

      when(fireboltStatementService.execute(any(), any(), any()))
          .thenReturn(mock(InputStream.class));
      fireboltStatement.executeQuery("SHOW DATABASE"); // call once to create queryId
      fireboltStatement.cancel();
      verify(fireboltStatementService, times(2))
          .execute(
              queryInfoWrapperArgumentCaptor.capture(),
              fireboltPropertiesArgumentCaptor.capture(),
              mapArgumentCaptor.capture());
      assertEquals(
          "KILL QUERY ON CLUSTER sql_cluster WHERE initial_query_id='"
              + queryInfoWrapperArgumentCaptor.getAllValues().get(0).getId()
              + "'",
          queryInfoWrapperArgumentCaptor.getAllValues().get(1).getSql());
      assertEquals(String.valueOf(0), mapArgumentCaptor.getValue().get("use_standard_sql"));
    }
  }

  @Test
  void shouldCancelByApiCallWhenAggressiveCancelIsDisabled() throws SQLException, IOException {
    try (MockedConstruction<FireboltResultSet> mockedResultSet =
        Mockito.mockConstruction(FireboltResultSet.class)) {

      FireboltProperties fireboltProperties =
          FireboltProperties.builder().database("db").additionalProperties(new HashMap<>()).build();

      FireboltStatement fireboltStatement =
          FireboltStatement.builder()
              .statementService(fireboltStatementService)
              .sessionProperties(fireboltProperties)
              .connection(fireboltConnection)
              .build();

      when(fireboltStatementService.execute(any(), any(), any()))
          .thenReturn(mock(InputStream.class));
      fireboltStatement.executeQuery("SHOW DATABASE"); // call once to create queryId
      fireboltStatement.cancel();
      verify(fireboltStatementService).abortStatement(any(), eq(fireboltProperties));
    }
  }

  @Test
  void shouldCloseInputStreamOnClose() throws SQLException, IOException {
    try (MockedConstruction<FireboltResultSet> mockedResultSet =
        Mockito.mockConstruction(FireboltResultSet.class)) {
      FireboltProperties fireboltProperties =
          FireboltProperties.builder().additionalProperties(new HashMap<>()).build();
      FireboltConnection connection = mock(FireboltConnection.class);
      FireboltStatement fireboltStatement =
          FireboltStatement.builder()
              .statementService(fireboltStatementService)
              .sessionProperties(fireboltProperties)
              .connection(connection)
              .build();

      when(fireboltStatementService.execute(any(), any(), any()))
          .thenReturn(mock(InputStream.class));

      fireboltStatement.executeQuery("show database");
      fireboltStatement.close();
      verify(mockedResultSet.constructed().get(0)).close();
      verify(connection).removeClosedStatement(fireboltStatement);
    }
  }
}
