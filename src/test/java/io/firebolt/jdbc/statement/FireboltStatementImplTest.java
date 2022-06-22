package io.firebolt.jdbc.statement;

import io.firebolt.jdbc.connection.FireboltConnectionTokens;
import io.firebolt.jdbc.connection.settings.FireboltProperties;
import io.firebolt.jdbc.resultset.FireboltResultSet;
import io.firebolt.jdbc.service.FireboltQueryService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.InputStream;
import java.sql.SQLException;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FireboltStatementImplTest {

  @Mock private FireboltQueryService fireboltQueryService;


  @Test
  void shouldExecuteQueryAndCreateResultSet() throws SQLException {
    try (MockedConstruction<FireboltResultSet> mocked =
        Mockito.mockConstruction(FireboltResultSet.class)) {
      FireboltProperties fireboltProperties =
          FireboltProperties.builder().additionalProperties(new HashMap<>()).build();
      FireboltConnectionTokens fireboltConnectionTokens =
          FireboltConnectionTokens.builder().accessToken("token").build();
      FireboltStatementImpl fireboltStatement =
          FireboltStatementImpl.builder()
              .fireboltQueryService(fireboltQueryService)
              .sessionProperties(fireboltProperties)
              .connectionTokens(fireboltConnectionTokens)
              .build();

      when(fireboltQueryService.executeQuery(
              eq("show database"), anyString(), eq("token"), eq(fireboltProperties)))
          .thenReturn(mock(InputStream.class));
      fireboltStatement.executeQuery("show database");
      assertTrue(fireboltProperties.getAdditionalProperties().isEmpty());
      verify(fireboltQueryService)
          .executeQuery(eq("show database"), anyString(), eq("token"), eq(fireboltProperties));
      assertEquals(1, mocked.constructed().size());
      assertEquals(-1, fireboltStatement.getUpdateCount());
    }
  }

  @Test
  void shouldExtractAdditionProperties() throws SQLException {
    try (MockedConstruction<FireboltResultSet> mockedResultSet =
        Mockito.mockConstruction(FireboltResultSet.class)) {
      FireboltProperties fireboltProperties =
          FireboltProperties.builder().additionalProperties(new HashMap<>()).build();
      FireboltConnectionTokens fireboltConnectionTokens =
          FireboltConnectionTokens.builder().accessToken("token").build();
      FireboltStatementImpl fireboltStatement =
          FireboltStatementImpl.builder()
              .fireboltQueryService(fireboltQueryService)
              .sessionProperties(fireboltProperties)
              .connectionTokens(fireboltConnectionTokens)
              .build();

      fireboltStatement.executeQuery("set custom_1 = 1");
      verifyNoMoreInteractions(fireboltQueryService);
      assertEquals(
              new HashMap<String, String>() {{
                put("custom_1", "1");
              }}, fireboltProperties.getAdditionalProperties());
      assertEquals(0, mockedResultSet.constructed().size());
    }
  }
}
