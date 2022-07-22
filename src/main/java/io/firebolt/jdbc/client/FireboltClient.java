package io.firebolt.jdbc.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.firebolt.jdbc.connection.FireboltConnection;
import io.firebolt.jdbc.connection.FireboltConnectionTokens;
import io.firebolt.jdbc.exception.FireboltException;
import io.firebolt.jdbc.resultset.compress.LZ4InputStream;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.cookie.StandardCookieSpec;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.util.Timeout;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static org.apache.hc.core5.http.HttpStatus.SC_SERVICE_UNAVAILABLE;
import static org.apache.hc.core5.http.HttpStatus.SC_UNAUTHORIZED;

@Getter
@Slf4j
public abstract class FireboltClient {

  public static final String HEADER_AUTHORIZATION = "Authorization";
  public static final String HEADER_AUTHORIZATION_BEARER_PREFIX_VALUE = "Bearer ";
  public static final String HEADER_USER_AGENT = "User-Agent";
  private final String headerUserAgentValue;

  private final CloseableHttpClient httpClient;

  private final FireboltConnection connection;

  protected final ObjectMapper objectMapper;

  protected FireboltClient(
      CloseableHttpClient httpClient,
      FireboltConnection connection,
      String customDrivers,
      String customClients,
      ObjectMapper objectMapper) {
    this.httpClient = httpClient;
    this.connection = connection;
    this.objectMapper = objectMapper;
    this.headerUserAgentValue =
        UsageTrackerUtil.getUserAgentString(
            customDrivers != null ? customDrivers : "", customClients != null ? customClients : "");
  }

  protected <T> T getResource(String uri, String host, Class<T> valueType)
      throws IOException, ParseException, FireboltException {
    HttpGet httpGet =
        createGetRequest(
            uri,
            this.getConnection()
                .getConnectionTokens()
                .map(FireboltConnectionTokens::getAccessToken)
                .orElse(null));
    try (CloseableHttpResponse response = this.execute(httpGet, host)) {
      String responseStr = EntityUtils.toString(response.getEntity());
      return objectMapper.readValue(responseStr, valueType);
    }
  }

  private HttpGet createGetRequest(String uri, String accessToken) {
    HttpGet httpGet = new HttpGet(uri);
    httpGet.setConfig(
        createRequestConfig(
            this.connection.getConnectionTimeout(), this.connection.getNetworkTimeout()));
    this.createHeaders(accessToken)
        .forEach(header -> httpGet.addHeader(header.getLeft(), header.getRight()));
    return httpGet;
  }

  protected CloseableHttpResponse execute(
      @NonNull HttpUriRequestBase httpUriRequestBase, String host)
      throws IOException, FireboltException {
    return execute(httpUriRequestBase, host, false);
  }

  protected CloseableHttpResponse execute(
      @NonNull HttpUriRequestBase httpUriRequestBase, String host, boolean isCompress)
      throws IOException, FireboltException {
    CloseableHttpResponse response;
    try {
      response = this.getHttpClient().execute(httpUriRequestBase);
      validateResponse(host, response, isCompress);
    } catch (Exception e) {
      EntityUtils.consumeQuietly(httpUriRequestBase.getEntity());
      throw e;
    }
    return response;
  }

  protected HttpPost createPostRequest(String uri) {
    return createPostRequest(uri, null);
  }

  protected HttpPost createPostRequest(String uri, String accessToken) {
    HttpPost httpPost = new HttpPost(uri);
    httpPost.setConfig(
        createRequestConfig(
            this.connection.getConnectionTimeout(), this.connection.getNetworkTimeout()));
    this.createHeaders(accessToken)
        .forEach(header -> httpPost.addHeader(header.getLeft(), header.getRight()));
    return httpPost;
  }

  protected void validateResponse(String host, CloseableHttpResponse response, Boolean isCompress)
      throws FireboltException {
    int statusCode = response.getCode();
    if (!isCallSuccessful(statusCode)) {
      if (statusCode == SC_SERVICE_UNAVAILABLE) {
        throw new FireboltException(
            String.format(
                "Could not query Firebolt at %s. The engine is not running. Status code: %d",
                host, HTTP_FORBIDDEN),
            statusCode);
      } else if (statusCode == SC_UNAUTHORIZED) {
        this.getConnection().removeExpiredTokens();
        throw new FireboltException(
            String.format(
                "Could not query Firebolt at %s. The token is expired and has been cleared", host),
            statusCode);
      }
      String errorResponseMessage;
      try {
        String errorFromResponse = extractErrorMessage(response.getEntity(), isCompress);
        errorResponseMessage =
            String.format(
                "Server failed to execute query with the following error:%n%s%ninternal error:%n%s",
                errorFromResponse, this.getInternalErrorWithHeadersText(response));
        throw new FireboltException(errorResponseMessage, statusCode);
      } catch (ParseException | IOException e) {
        log.warn("Could not parse response containing the error message from Firebolt", e);
        errorResponseMessage =
            String.format(
                "Server failed to execute query%ninternal error:%n%s",
                this.getInternalErrorWithHeadersText(response));
        throw new FireboltException(errorResponseMessage, statusCode, e);
      }
    }
  }

  private String extractErrorMessage(HttpEntity entity, boolean isCompress)
      throws IOException, ParseException {
    byte[] entityBytes = EntityUtils.toByteArray(entity);
    if (isCompress) {
      try {
        InputStream is = new LZ4InputStream(new ByteArrayInputStream(entityBytes));
        return new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
                .lines()
                .collect(Collectors.joining("\n"))
            + "\n";
      } catch (Exception e) {
        log.warn("Could not decompress error from server");
      }
    }
    return entityBytes != null ? new String(entityBytes, StandardCharsets.UTF_8) : null;
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
    return response.toString() + "\n" + Arrays.toString(response.getHeaders());
  }

  private RequestConfig createRequestConfig(int connectionTimeoutMillis, int networkTimoutMillis) {
    return RequestConfig.custom()
        .setConnectTimeout(Timeout.of(connectionTimeoutMillis, TimeUnit.MILLISECONDS))
        .setCookieSpec(StandardCookieSpec.RELAXED)
        .setResponseTimeout(Timeout.ofMilliseconds(networkTimoutMillis))
        .build();
  }
}
