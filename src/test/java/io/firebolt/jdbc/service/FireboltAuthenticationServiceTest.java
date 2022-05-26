package io.firebolt.jdbc.service;

import io.firebolt.jdbc.client.authentication.FireboltAuthenticationClient;
import io.firebolt.jdbc.connection.FireboltConnectionTokens;
import org.apache.hc.core5.http.ParseException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FireboltAuthenticationServiceTest {

  private static final String USER = "usr";
  private static final String PASSWORD = "PA§§WORD";

  private static final String HOST = "HOST";

  @Mock private FireboltAuthenticationClient fireboltAuthenticationClient;

  private FireboltAuthenticationService fireboltAuthenticationService;

  @BeforeEach
  void setUp() {
    fireboltAuthenticationService = new FireboltAuthenticationService(fireboltAuthenticationClient);
  }

  @Test
  void shouldGetConnectionToken() throws IOException, ParseException {
    FireboltConnectionTokens tokens =
        FireboltConnectionTokens.builder()
            .expiresInSeconds(52)
            .refreshToken("refresh")
            .accessToken("access")
            .build();
    when(fireboltAuthenticationClient.postConnectionTokens(HOST, USER, PASSWORD))
        .thenReturn(tokens);

    assertEquals(tokens, fireboltAuthenticationService.getConnectionTokens(HOST, USER, PASSWORD));
    verify(fireboltAuthenticationClient).postConnectionTokens(HOST, USER, PASSWORD);
  }

  @Test
  void shouldCallClientOnlyOnceWhenServiceCalledTwiceForTheSameHost()
      throws IOException, ParseException {
    FireboltConnectionTokens tokens =
        FireboltConnectionTokens.builder()
            .expiresInSeconds(52)
            .refreshToken("refresh")
            .accessToken("access")
            .build();
    when(fireboltAuthenticationClient.postConnectionTokens(HOST, USER, PASSWORD))
        .thenReturn(tokens);

    fireboltAuthenticationService.getConnectionTokens(HOST, USER, PASSWORD);
    assertEquals(tokens, fireboltAuthenticationService.getConnectionTokens(HOST, USER, PASSWORD));
    verify(fireboltAuthenticationClient).postConnectionTokens(HOST, USER, PASSWORD);
  }

  @Test
  void shouldCallClientAgainWhenTokenIsExpired()
      throws IOException, NoSuchAlgorithmException, InterruptedException, ParseException {
    FireboltConnectionTokens tokens =
        FireboltConnectionTokens.builder()
            .expiresInSeconds(1)
            .refreshToken("refresh")
            .accessToken("access")
            .build();
    when(fireboltAuthenticationClient.postConnectionTokens(HOST, USER, PASSWORD))
        .thenReturn(tokens);
    fireboltAuthenticationService.getConnectionTokens(HOST, USER, PASSWORD);
    TimeUnit.MILLISECONDS.sleep(1100);
    assertEquals(tokens, fireboltAuthenticationService.getConnectionTokens(HOST, USER, PASSWORD));
    verify(fireboltAuthenticationClient, times(2)).postConnectionTokens(HOST, USER, PASSWORD);
  }
}
