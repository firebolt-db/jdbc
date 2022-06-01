package io.firebolt.jdbc.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.firebolt.jdbc.ProjectVersionUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;

@Getter
@Slf4j
public abstract class FireboltClient {

  public static final String HEADER_AUTHORIZATION = "Authorization";
  public static final String HEADER_AUTHORIZATION_BEARER_PREFIX_VALUE = "Bearer ";
  public static final String HEADER_USER_AGENT = "User-Agent";
  private final String headerUserAgentValue;

  protected FireboltClient() {
    String version = ProjectVersionUtil.getProjectVersion();
    this.headerUserAgentValue = String.format("fireboltJdbcDriver/%s", version);
  }

  protected <T> T getResource(
      String uri,
      String accessToken,
      CloseableHttpClient httpClient,
      ObjectMapper objectMapper,
      Class<T> valueType)
      throws IOException, ParseException {
    HttpGet httpGet = createGetRequest(uri, accessToken);
    try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
      this.validateResponse(uri, response);
      String responseStr = EntityUtils.toString(response.getEntity());
      return objectMapper.readValue(responseStr, valueType);
    } catch (Exception e) {
      EntityUtils.consumeQuietly(httpGet.getEntity());
      throw e;
    }
  }

  private HttpGet createGetRequest(String uri, String accessToken) {
    HttpGet httpGet = new HttpGet(uri);
    this.createHeaders(accessToken)
        .forEach(header -> httpGet.addHeader(header.getLeft(), header.getRight()));
    return httpGet;
  }

  protected HttpPost createPostRequest(String uri, String accessToken) {
    HttpPost httpPost = new HttpPost(uri);
    this.createHeaders(accessToken)
        .forEach(header -> httpPost.addHeader(header.getLeft(), header.getRight()));
    return httpPost;
  }

  protected void validateResponse(String uri, CloseableHttpResponse response) throws IOException {
    int statusCode = response.getCode();
    if (!isCallSuccessful(statusCode)) {
      if (statusCode == HTTP_UNAVAILABLE) {
        throw new IOException(
            String.format(
                "Could not query Firebolt at %s. The engine is not running. Status code: %d",
                uri, HTTP_FORBIDDEN));
      }
      String errorResponseMessage;
      try {
        errorResponseMessage =
            String.format(
                "Failed to query Firebolt at %s. Status code: %d, Error message: %s",
                uri, statusCode, EntityUtils.toString(response.getEntity()));
      } catch (ParseException e) {
        log.warn("Could not parse response containing the error message from Firebolt", e);
        errorResponseMessage =
            String.format("Failed to query Firebolt with uri %s. Status code: %d", uri, statusCode);
      }
      throw new IOException(errorResponseMessage);
    }
  }

  private boolean isCallSuccessful(int statusCode) {
    return statusCode >= 200
        && statusCode <= 299; // Call is considered successful when the status code is 2XX
  }

  private List<Pair<String, String>> createHeaders(String accessToken) {
    List<Pair<String, String>> headers = new ArrayList<>();
    headers.add(new ImmutablePair<>(HEADER_USER_AGENT, this.getHeaderUserAgentValue()));
    headers.add(
        new ImmutablePair<>(
            HEADER_AUTHORIZATION, HEADER_AUTHORIZATION_BEARER_PREFIX_VALUE + accessToken));
    return headers;
  }
}
