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
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.Optional;

import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;

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
      int statusCode =
          Optional.ofNullable(response.getStatusLine()).map(StatusLine::getStatusCode).orElse(-1);
      String responseStr = EntityUtils.toString(response.getEntity());
      log.debug(
          "POST {} - Http status code : {}, response : {}", connectUrl, statusCode, responseStr);

      if (!(statusCode >= 200 && statusCode <= 299)) {
        if (statusCode == HTTP_NOT_FOUND) {
          throw new IOException(
              String.format(
                  "Could not get connection tokens (error %d), response: %s",
                  HTTP_NOT_FOUND, responseStr));
        }
        if (statusCode == HTTP_FORBIDDEN) {
          throw new IOException(
              String.format(
                  "Authentication failed (error %d), please verify your credentials. Response: %s",
                  HTTP_FORBIDDEN, responseStr));
        }
        throw new IOException(
            String.format(
                "Failed to connect to Firebolt. status code: %d, Response: %s",
                statusCode, responseStr));
      }
      FireboltAuthenticationResponse authenticationResponse =
          objectMapper.readValue(responseStr, FireboltAuthenticationResponse.class);
      FireboltConnectionTokens authenticationTokens =
          FireboltConnectionTokens.builder()
              .accessToken(authenticationResponse.getAccessToken())
              .refreshToken(authenticationResponse.getRefreshToken())
              .expiresInSeconds(authenticationResponse.getExpiresIn())
              .build();
      log.info("Http connection created");
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
