package io.firebolt.jdbc.client.query;

import io.firebolt.jdbc.client.FireboltClient;
import io.firebolt.jdbc.connection.settings.FireboltProperties;
import io.firebolt.jdbc.exception.FireboltException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicNameValuePair;
import org.apache.hc.core5.net.URIBuilder;

import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.*;

@RequiredArgsConstructor
@Slf4j
public class QueryClientImpl extends FireboltClient implements QueryClient {
  private final CloseableHttpClient httpClient;

  private final Map<String, HttpPost> runningQueries = new HashMap<>();

  public InputStream postSqlQuery(
      String sql,
      boolean isSelect,
      String queryId,
      String accessToken,
      FireboltProperties fireboltProperties)
      throws FireboltException {
    HttpEntity requestEntity = null;
    try {
      List<NameValuePair> queryParameters =
          this.getQueryParameters(fireboltProperties, queryId, isSelect);
      String uri = this.buildQueryUri(fireboltProperties, queryParameters).toString();
      requestEntity = new StringEntity(sql, StandardCharsets.UTF_8);
      HttpPost post = this.createPostRequest(uri, accessToken);
      runningQueries.put(queryId, post);
      post.setEntity(requestEntity);
      log.debug("Posting query to URI: {}", uri);
      CloseableHttpResponse response = httpClient.execute(post);
      validateResponse(uri, response);
      return response.getEntity().getContent();
    } catch (Exception e) {
      EntityUtils.consumeQuietly(requestEntity);
      throw new FireboltException(
          String.format("Error executing query %s. Error: %s", sql, e.getMessage()), e);
    } finally {
      runningQueries.remove(queryId);
    }
  }

  public void postCancelSqlQuery(String queryId, FireboltProperties fireboltProperties)
      throws FireboltException {
    try {
      abortCallWithQueryId(queryId);

      BasicNameValuePair queryIdParam = new BasicNameValuePair("query_id", queryId);
      String uri =
          this.buildCancelUri(fireboltProperties, Collections.singletonList(queryIdParam))
              .toString();
      HttpPost post = new HttpPost(uri);
      CloseableHttpResponse response = httpClient.execute(post);
      validateResponse(uri, response);
    } catch (Exception e) {
      throw new FireboltException(
          String.format("Could not cancel query %s, error %s", queryId, e.getMessage()), e);
    }
  }

  private void abortCallWithQueryId(String queryId) {
    try {
      if (runningQueries.containsKey(queryId)) {
        runningQueries.get(queryId).abort();
      }
    } catch (Exception e) {
      log.error("Could not abort running query with id {}", queryId);
    }
  }

  private List<NameValuePair> getQueryParameters(
      FireboltProperties fireboltProperties, String queryId, boolean isSelect) {
    List<NameValuePair> queryParams = new ArrayList<>();

    fireboltProperties
        .getAdditionalProperties()
        .forEach((k, v) -> queryParams.add(new BasicNameValuePair(k, v)));

    if (isSelect && !StringUtils.equalsIgnoreCase("localhost", fireboltProperties.getHost())) {
      queryParams.add(new BasicNameValuePair("output_format", "TabSeparatedWithNamesAndTypes"));
    }
    queryParams.add(new BasicNameValuePair("database", fireboltProperties.getDatabase()));
    queryParams.add(new BasicNameValuePair("query_id", queryId));
    queryParams.add(
        new BasicNameValuePair("compress", String.format("%d", fireboltProperties.getCompress())));
    return queryParams;
  }

  private URI buildQueryUri(
      FireboltProperties fireboltProperties, List<NameValuePair> queryParameters)
      throws URISyntaxException {

    return new URIBuilder()
        .setScheme(Boolean.TRUE.equals(fireboltProperties.getSsl()) ? "https" : "http")
        .setHost(fireboltProperties.getHost())
        .setPort(fireboltProperties.getPort())
        .setPath("/")
        .setParameters(queryParameters)
        .build();
  }

  private URI buildCancelUri(
      FireboltProperties fireboltProperties, List<NameValuePair> queryParameters)
      throws URISyntaxException {

    return new URIBuilder()
        .setScheme(Boolean.TRUE.equals(fireboltProperties.getSsl()) ? "https" : "http")
        .setHost(fireboltProperties.getHost())
        .setPort(fireboltProperties.getPort())
        .setPath("/cancel")
        .setParameters(queryParameters)
        .build();
  }
}
