package io.firebolt.jdbc.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.firebolt.jdbc.connection.FireboltConnection;
import io.firebolt.jdbc.connection.FireboltConnectionTokens;
import io.firebolt.jdbc.exception.FireboltException;
import io.firebolt.jdbc.resultset.compress.LZ4InputStream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.firebolt.jdbc.exception.ExceptionType.EXPIRED_TOKEN;
import static java.net.HttpURLConnection.*;

@Getter
@Slf4j
public abstract class FireboltClient {

  public static final String HEADER_AUTHORIZATION = "Authorization";
  public static final String HEADER_AUTHORIZATION_BEARER_PREFIX_VALUE = "Bearer ";
  public static final String HEADER_USER_AGENT = "User-Agent";
  private final String headerUserAgentValue;

  private final CloseableHttpClient httpClient;

  private final FireboltConnection connection;

  protected FireboltClient(CloseableHttpClient httpClient, FireboltConnection connection,
                           String customDrivers,
                           String customClients) {
    this.httpClient = httpClient;
    this.connection = connection;
    this.headerUserAgentValue =
        UsageTrackerUtil.getUserAgentString(
            customDrivers != null ? customDrivers : "", customClients != null ? customClients : "");
  }

  protected <T> T getResource(
      String uri,
      String host,
      CloseableHttpClient httpClient,
      ObjectMapper objectMapper,
      Class<T> valueType,
      boolean isCompress)
      throws IOException, ParseException, FireboltException {
    HttpGet httpGet =
        createGetRequest(
            uri,
            this.getConnection()
                .getConnectionTokens()
                .map(FireboltConnectionTokens::getAccessToken)
                .orElse(null));
    try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
      this.validateResponse(host, response, isCompress);
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

  protected void validateResponse(String host, CloseableHttpResponse response, Boolean isCompress)
      throws FireboltException {
    int statusCode = response.getCode();
    if (!isCallSuccessful(statusCode)) {
      if (statusCode == HTTP_UNAVAILABLE) {
        throw new FireboltException(
            String.format(
                "Could not query Firebolt at %s. The engine is not running. Status code: %d",
                host, HTTP_FORBIDDEN));
      } else if (statusCode == HTTP_UNAUTHORIZED) {
        this.getConnection().removeExpiredTokens();
        throw new FireboltException(
            String.format(
                "Could not query Firebolt at %s. The token is expired and has been cleared", host),
            EXPIRED_TOKEN);
      }
      String errorResponseMessage;
      try {
        String errorFromResponse = extractErrorMessage(response.getEntity(), isCompress);
        errorResponseMessage =
            String.format(
                "Server failed to execute query with the following error:%n%s%ninternal error:%n%s",
                errorFromResponse, this.getInternalErrorWithHeadersText(response));

        throw new FireboltException(errorResponseMessage);
      } catch (ParseException | IOException e) {
        log.warn("Could not parse response containing the error message from Firebolt", e);
        errorResponseMessage =
            String.format(
                "Server failed to execute query%ninternal error:%n%s",
                this.getInternalErrorWithHeadersText(response));
        throw new FireboltException(errorResponseMessage, e);
      }
    }
  }

  private String extractErrorMessage(HttpEntity entity, boolean isCompress)
      throws IOException, ParseException {
    if (isCompress) {
      InputStream is =
          new LZ4InputStream(new ByteArrayInputStream(EntityUtils.toByteArray(entity)));
      return new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
              .lines()
              .collect(Collectors.joining("\n"))
          + "\n";
    } else {
      return EntityUtils.toString(entity);
    }
  }

  private boolean isCallSuccessful(int statusCode) {
    return statusCode >= 200
        && statusCode <= 299; // Call is considered successful when the status code is 2XX
  }

  private List<Pair<String, String>> createHeaders(String accessToken) {
    List<Pair<String, String>> headers = new ArrayList<>();
    headers.add(new ImmutablePair<>(HEADER_USER_AGENT, this.getHeaderUserAgentValue()));
    Optional.ofNullable(accessToken)
        .ifPresent(
            token ->
                headers.add(
                    new ImmutablePair<>(
                        HEADER_AUTHORIZATION,
                        HEADER_AUTHORIZATION_BEARER_PREFIX_VALUE + accessToken)));
    return headers;
  }

  private String getInternalErrorWithHeadersText(CloseableHttpResponse response) {
    StringBuilder stringBuilder = new StringBuilder(response.toString());
    stringBuilder.append("\n");
    stringBuilder.append(Arrays.toString(response.getHeaders()));
    return stringBuilder.toString();
  }

}
