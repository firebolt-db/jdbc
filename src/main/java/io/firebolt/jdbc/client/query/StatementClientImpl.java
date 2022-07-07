package io.firebolt.jdbc.client.query;

import io.firebolt.jdbc.statement.StatementInfoWrapper;
import io.firebolt.jdbc.statement.StatementUtil;
import io.firebolt.jdbc.client.FireboltClient;
import io.firebolt.jdbc.connection.FireboltConnection;
import io.firebolt.jdbc.connection.FireboltConnectionTokens;
import io.firebolt.jdbc.connection.settings.FireboltProperties;
import io.firebolt.jdbc.exception.FireboltException;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class StatementClientImpl extends FireboltClient implements StatementClient {
  private final Map<String, HttpPost> runningQueries = new HashMap<>();

  public StatementClientImpl(
      CloseableHttpClient httpClient,
      FireboltConnection connection,
      String customDrivers,
      String customClients) {
    super(httpClient, connection, customDrivers, customClients);
  }

  @Override
  public InputStream postSqlStatement(
      @NonNull StatementInfoWrapper statementInfoWrapper,
      @NonNull FireboltProperties connectionProperties,
      Map<String, String> statementParams)
      throws FireboltException {
    HttpEntity requestEntity = null;
    String formattedStatement = formatStatement(statementInfoWrapper.getSql());
    try {
      List<NameValuePair> parameters =
              statementParams.entrySet().stream()
              .map(e -> new BasicNameValuePair(e.getKey(), e.getValue()))
              .collect(Collectors.toList());
      String uri = this.buildQueryUri(connectionProperties, parameters).toString();
      requestEntity = new StringEntity(formattedStatement, StandardCharsets.UTF_8);
      HttpPost post =
          this.createPostRequest(
              uri,
              this.getConnection()
                  .getConnectionTokens()
                  .map(FireboltConnectionTokens::getAccessToken)
                  .orElse(null));
      runningQueries.put(statementInfoWrapper.getId(), post);
      post.setEntity(requestEntity);
      log.debug("Posting statement with id {} to URI: {}", statementInfoWrapper.getId(), uri);
      CloseableHttpResponse response = this.getHttpClient().execute(post);
      validateResponse(connectionProperties.getHost(), response, connectionProperties.isCompress());
      log.debug("Statement with id {} was successfully posted", statementInfoWrapper.getId());
      return response.getEntity().getContent();
    } catch (FireboltException e) {
      EntityUtils.consumeQuietly(requestEntity);
      throw e;
    } catch (Exception e) {
      EntityUtils.consumeQuietly(requestEntity);
      throw new FireboltException(String.format("Error executing statement %s", formattedStatement), e);
    } finally {
      runningQueries.remove(statementInfoWrapper.getId());
    }
  }

  private String formatStatement(String sql) {
    String cleaned = StatementUtil.cleanQuery(sql);
    if (!cleaned.endsWith(";")) {
      cleaned += ";";
    }
    return cleaned;
  }

  public void postCancelSqlStatement(
      String id, FireboltProperties fireboltProperties, Map<String, String> statementParams)
      throws FireboltException {
    try {
      abortCallWithId(id);

      List<NameValuePair> params =
              statementParams.entrySet().stream()
              .map(kv -> new BasicNameValuePair(kv.getKey(), kv.getValue()))
              .collect(Collectors.toList());

      String uri = this.buildCancelUri(fireboltProperties, params).toString();
      HttpPost post = new HttpPost(uri);
      CloseableHttpResponse response = this.getHttpClient().execute(post);
      validateResponse(fireboltProperties.getHost(), response, fireboltProperties.isCompress());
    } catch (Exception e) {
      throw new FireboltException(
          String.format("Could not cancel query: %s at %s", id, fireboltProperties.getHost()),
          e);
    }
  }

  private void abortCallWithId(@NonNull String id) {
    try {
      if (runningQueries.containsKey(id)) {
        runningQueries.get(id).abort();
      }
    } catch (Exception e) {
      log.error("Could not abort running statement with id {}", id);
    }
  }

  private URI buildQueryUri(
      FireboltProperties fireboltProperties, List<NameValuePair> parameters)
      throws URISyntaxException {

    return new URIBuilder()
        .setScheme(Boolean.TRUE.equals(fireboltProperties.isSsl()) ? "https" : "http")
        .setHost(fireboltProperties.getHost())
        .setPort(fireboltProperties.getPort())
        .setPath("/")
        .setParameters(parameters)
        .build();
  }

  private URI buildCancelUri(
      FireboltProperties fireboltProperties, List<NameValuePair> parameters)
      throws URISyntaxException {

    return new URIBuilder()
        .setScheme(Boolean.TRUE.equals(fireboltProperties.isSsl()) ? "https" : "http")
        .setHost(fireboltProperties.getHost())
        .setPort(fireboltProperties.getPort())
        .setPath("/cancel")
        .setParameters(parameters)
        .build();
  }

}
