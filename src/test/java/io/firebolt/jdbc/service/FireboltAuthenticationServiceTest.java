package io.firebolt.jdbc.service;

import io.firebolt.jdbc.client.authentication.FireboltAuthenticationClient;
import io.firebolt.jdbc.connection.FireboltConnectionTokens;
import io.firebolt.jdbc.exception.FireboltException;
import org.apache.hc.core5.http.ParseException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FireboltAuthenticationServiceTest {

  private static final String USER = "usr";
  private static final String PASSWORD = "PA§§WORD";

  @Mock private FireboltAuthenticationClient fireboltAuthenticationClient;

  private FireboltAuthenticationService fireboltAuthenticationService;

  @BeforeEach
  void setUp() {
    fireboltAuthenticationService = new FireboltAuthenticationService(fireboltAuthenticationClient);
  }

  @Test
  void shouldGetConnectionToken() throws IOException, ParseException, FireboltException {
    String randomHost = UUID.randomUUID().toString();
    FireboltConnectionTokens tokens =
        FireboltConnectionTokens.builder()
            .expiresInSeconds(52)
            .refreshToken("refresh")
            .accessToken("access")
            .build();
    when(fireboltAuthenticationClient.postConnectionTokens(randomHost, USER, PASSWORD))
        .thenReturn(tokens);

    assertEquals(
        tokens, fireboltAuthenticationService.getConnectionTokens(randomHost, USER, PASSWORD));
    verify(fireboltAuthenticationClient).postConnectionTokens(randomHost, USER, PASSWORD);
  }

  @Test
  void shouldCallClientOnlyOnceWhenServiceCalledTwiceForTheSameHost()
      throws IOException, ParseException, FireboltException {
    String randomHost = UUID.randomUUID().toString();
    FireboltConnectionTokens tokens =
        FireboltConnectionTokens.builder()
            .expiresInSeconds(52)
            .refreshToken("refresh")
            .accessToken("access")
            .build();
    when(fireboltAuthenticationClient.postConnectionTokens(randomHost, USER, PASSWORD))
        .thenReturn(tokens);

    fireboltAuthenticationService.getConnectionTokens(randomHost, USER, PASSWORD);
    assertEquals(
        tokens, fireboltAuthenticationService.getConnectionTokens(randomHost, USER, PASSWORD));
    verify(fireboltAuthenticationClient).postConnectionTokens(randomHost, USER, PASSWORD);
  }

  @Test
  void shouldCallClientAgainWhenTokenIsExpired()
      throws IOException, InterruptedException, ParseException, FireboltException {
    String randomHost = UUID.randomUUID().toString();
    FireboltConnectionTokens tokens =
        FireboltConnectionTokens.builder()
            .expiresInSeconds(1)
            .refreshToken("refresh")
            .accessToken("access")
            .build();
    when(fireboltAuthenticationClient.postConnectionTokens(randomHost, USER, PASSWORD))
        .thenReturn(tokens);
    fireboltAuthenticationService.getConnectionTokens(randomHost, USER, PASSWORD);
    TimeUnit.MILLISECONDS.sleep(1100);
    assertEquals(
        tokens, fireboltAuthenticationService.getConnectionTokens(randomHost, USER, PASSWORD));
    verify(fireboltAuthenticationClient, times(2)).postConnectionTokens(randomHost, USER, PASSWORD);
  }
}
