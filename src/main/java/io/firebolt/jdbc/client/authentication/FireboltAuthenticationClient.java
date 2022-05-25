package io.firebolt.jdbc.client.authentication;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.firebolt.jdbc.client.FireboltClient;
import io.firebolt.jdbc.client.authentication.response.FireboltAuthenticationResponse;
import io.firebolt.jdbc.connection.FireboltConnectionTokens;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

@Slf4j
@RequiredArgsConstructor
public class FireboltAuthenticationClient extends FireboltClient {
  private static final String USERNAME = "username";
  private static final String PASSWORD = "password";
  private static final String AUTH_URL = "%s/auth/v1/login";
  private final CloseableHttpClient httpClient;
  private final ObjectMapper objectMapper;

  public FireboltConnectionTokens postConnectionTokens(String host, String user, String password)
      throws IOException {
    String connectUrl = String.format(AUTH_URL, host);
    log.debug("Creating connection with url {}", connectUrl);
    HttpPost post = new HttpPost(connectUrl);
    post.addHeader(HEADER_USER_AGENT, this.getHeaderUserAgentValue());
    post.setEntity(new StringEntity(createLoginRequest(user, password)));

    try (CloseableHttpResponse response = httpClient.execute(post)) {
      this.validateResponse(connectUrl, response);
      String responseStr = EntityUtils.toString(response.getEntity());
      FireboltAuthenticationResponse authenticationResponse =
          objectMapper.readValue(responseStr, FireboltAuthenticationResponse.class);
      FireboltConnectionTokens authenticationTokens =
          FireboltConnectionTokens.builder()
              .accessToken(authenticationResponse.getAccessToken())
              .refreshToken(authenticationResponse.getRefreshToken())
              .expiresInSeconds(authenticationResponse.getExpiresIn())
              .build();
      log.info("Successfully fetched connection tokens");
      logToken(authenticationResponse);
      return authenticationTokens;
    }
  }

  private void logToken(FireboltAuthenticationResponse connectionTokens) {

    if (!StringUtils.isEmpty(connectionTokens.getAccessToken())) {
      log.debug("Retrieved access_token");
    }

    if (!StringUtils.isEmpty(connectionTokens.getRefreshToken())) {
      log.debug("Retrieved refresh_token");
    }
    if (0 <= connectionTokens.getExpiresIn()) {
      log.debug("Retrieved expires_in");
    }
  }

  private String createLoginRequest(String username, String password)
      throws JsonProcessingException {
    return new ObjectMapper()
        .writeValueAsString(ImmutableMap.of(USERNAME, username, PASSWORD, password));
  }
}
