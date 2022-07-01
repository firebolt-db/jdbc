package io.firebolt.jdbc.client.query;

import io.firebolt.QueryUtil;
import io.firebolt.jdbc.client.FireboltClient;
import io.firebolt.jdbc.connection.FireboltConnection;
import io.firebolt.jdbc.connection.FireboltConnectionTokens;
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

@Slf4j
public class QueryClientImpl extends FireboltClient implements QueryClient {
  private static final String TAB_SEPARATED_WITH_NAMES_AND_TYPES_FORMAT =
      "TabSeparatedWithNamesAndTypes";
  private final Map<String, HttpPost> runningQueries = new HashMap<>();

  public QueryClientImpl(
      CloseableHttpClient httpClient,
      FireboltConnection connection,
      String customDrivers,
      String customClients) {
    super(httpClient, connection, customDrivers, customClients);
  }

  public InputStream postSqlQuery(
      String sql,
      boolean isSelect,
      String queryId,
      FireboltProperties fireboltProperties)
      throws FireboltException {
    HttpEntity requestEntity = null;
    String formattedQuery = formatQuery(sql);
    try {
      List<NameValuePair> queryParameters =
          this.getQueryParameters(fireboltProperties, queryId, isSelect);
      String uri = this.buildQueryUri(fireboltProperties, queryParameters).toString();
      requestEntity = new StringEntity(formattedQuery, StandardCharsets.UTF_8);
      HttpPost post = this.createPostRequest(uri, this.getConnection().getConnectionTokens().map(FireboltConnectionTokens::getAccessToken).orElse(null));
      runningQueries.put(queryId, post);
      post.setEntity(requestEntity);
      log.debug("Posting query with id {} to URI: {}", queryId, uri);
      CloseableHttpResponse response = this.getHttpClient().execute(post);
      validateResponse(fireboltProperties.getHost(), response, fireboltProperties.isCompress());
      log.debug("Query with id {} was successfully posted", queryId);
      return response.getEntity().getContent();
    } catch (FireboltException e) {
      EntityUtils.consumeQuietly(requestEntity);
      throw e;
    } catch (Exception e) {
      EntityUtils.consumeQuietly(requestEntity);
      throw new FireboltException(String.format("Error executing query %s", formattedQuery), e);
    } finally {
      runningQueries.remove(queryId);
    }
  }

  private String formatQuery(String sql) {
    String cleaned = QueryUtil.cleanQuery(sql);
    if (!cleaned.endsWith(";")) {
      cleaned += ";";
    }
    return cleaned;
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
      CloseableHttpResponse response = this.getHttpClient().execute(post);
      validateResponse(fireboltProperties.getHost(), response, fireboltProperties.isCompress());
    } catch (Exception e) {
      throw new FireboltException(
          String.format("Could not cancel query: %s at %s", queryId, fireboltProperties.getHost()),
          e);
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
    boolean isLocalDb = StringUtils.equalsIgnoreCase("localhost", fireboltProperties.getHost());

    fireboltProperties
        .getAdditionalProperties()
        .forEach((key, value) -> queryParams.add(new BasicNameValuePair(key, value)));

    getResponseFormatQueryParam(isSelect, isLocalDb).ifPresent(queryParams::add);

    queryParams.add(new BasicNameValuePair("database", fireboltProperties.getDatabase()));
    queryParams.add(new BasicNameValuePair("query_id", queryId));
    queryParams.add(
        new BasicNameValuePair("compress", String.format("%d", fireboltProperties.getCompress())));
    return queryParams;
  }

  private Optional<BasicNameValuePair> getResponseFormatQueryParam(
      boolean isSelect, boolean isLocalDb) {
    if (isSelect) {
      if (isLocalDb) {
        return Optional.of(
            new BasicNameValuePair("default_format", TAB_SEPARATED_WITH_NAMES_AND_TYPES_FORMAT));
      } else {
        return Optional.of(
            new BasicNameValuePair("output_format", TAB_SEPARATED_WITH_NAMES_AND_TYPES_FORMAT));
      }
    }
    return Optional.empty();
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
