package io.firebolt.jdbc.connection;

import io.firebolt.jdbc.exception.FireboltException;
import io.firebolt.jdbc.service.FireboltAuthenticationService;
import io.firebolt.jdbc.service.FireboltEngineService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FireboltConnectionImplTest {

  private final FireboltConnectionTokens fireboltConnectionTokens =
      FireboltConnectionTokens.builder().build();
  @Mock private FireboltAuthenticationService fireboltAuthenticationService;
  @Mock private FireboltEngineService fireboltEngineService;

  @Test
  void shouldInitConnection() throws FireboltException {
    String uri = "jdbc:firebolt://firebolt.io/db";
    Properties connectionProperties = new Properties();
    connectionProperties.put("user", "user");
    connectionProperties.put("password", "pa$$word");
    connectionProperties.put("host", "firebolt.io");

    when(fireboltAuthenticationService.getConnectionTokens(
            "https://firebolt.io", "user", "pa$$word"))
        .thenReturn(fireboltConnectionTokens);
    FireboltConnectionImpl fireboltConnectionImpl =
        new FireboltConnectionImpl(
                uri, connectionProperties, fireboltAuthenticationService, fireboltEngineService)
            .connect();
    verify(fireboltAuthenticationService)
        .getConnectionTokens("https://firebolt.io", "user", "pa$$word");
    assertNotNull(fireboltConnectionImpl);
  }
}
