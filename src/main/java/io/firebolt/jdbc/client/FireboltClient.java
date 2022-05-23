package io.firebolt.jdbc.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.firebolt.jdbc.ProjectVersionUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.Optional;

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
      String identifier,
      String accessToken,
      CloseableHttpClient httpClient,
      ObjectMapper objectMapper,
      Class<T> valueType)
      throws IOException {
    HttpGet httpGet = createGetRequest(uri, accessToken);
    try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
      this.validateResponse(uri, response);
      String responseStr = EntityUtils.toString(response.getEntity());
      return objectMapper.readValue(responseStr, valueType);
    }
  }

  private HttpGet createGetRequest(String uri, String accessToken) {
    HttpGet httpGet = new HttpGet(uri);
    httpGet.addHeader(HEADER_USER_AGENT, this.getHeaderUserAgentValue());
    httpGet.addHeader(HEADER_AUTHORIZATION, HEADER_AUTHORIZATION_BEARER_PREFIX_VALUE + accessToken);
    return httpGet;
  }

  protected HttpPost createPostRequest(String uri, String accessToken) {
    HttpPost httpPost = new HttpPost(uri);
    httpPost.addHeader(HEADER_USER_AGENT, this.getHeaderUserAgentValue());
    httpPost.addHeader(
        HEADER_AUTHORIZATION, HEADER_AUTHORIZATION_BEARER_PREFIX_VALUE + accessToken);
    return httpPost;
  }

  protected void validateResponse(String uri, CloseableHttpResponse response) throws IOException {
    int statusCode = 
        Optional.ofNullable(response.getStatusLine()).map(StatusLine::getStatusCode).orElse(-1);
    if (!isCallSuccessful(statusCode)) {
      if (statusCode == HTTP_UNAVAILABLE) {
        throw new IOException(
            String.format(
                "Could not query Firebolt with uri %s. The engine is NOT running. Status code : %d",
                uri, HTTP_FORBIDDEN));
      }
      throw new IOException(
          String.format(
              "Failed to query Firebolt with uri %s. Status code: %d, Error message: %s",
              uri, statusCode, EntityUtils.toString(response.getEntity())));
    }
  }

  private boolean isCallSuccessful(int statusCode) {
    return statusCode >= 200 && statusCode <= 299; // Call is considered successful when the status code is 2XX
  }
}
