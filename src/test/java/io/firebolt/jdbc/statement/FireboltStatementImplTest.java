package io.firebolt.jdbc.statement;

import io.firebolt.jdbc.connection.FireboltConnectionImpl;
import io.firebolt.jdbc.connection.FireboltConnectionTokens;
import io.firebolt.jdbc.connection.settings.FireboltProperties;
import io.firebolt.jdbc.resultset.FireboltResultSet;
import io.firebolt.jdbc.service.FireboltQueryService;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.InputStream;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FireboltStatementImplTest {

  @Mock private FireboltQueryService fireboltQueryService;

  @Mock private FireboltConnectionImpl fireboltConnectionImpl;

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
    }
  }

  @Test
  void shouldExtractAdditionProperties() throws SQLException {
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

      when(fireboltQueryService.extractAdditionalProperties(("set custom_1 = 1")))
          .thenReturn(Optional.of(new ImmutablePair<>("custom_1", "1")));
      fireboltStatement.executeQuery("set custom_1 = 1");
      verifyNoMoreInteractions(fireboltQueryService);
      assertEquals(
          fireboltProperties.getAdditionalProperties(),
              new HashMap<String, String>() {{
                put("custom_1", "1");
              }});
      assertEquals(0, mocked.constructed().size());
    }
  }
}
