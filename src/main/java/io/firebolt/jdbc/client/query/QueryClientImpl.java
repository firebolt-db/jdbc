package io.firebolt.jdbc.client.query;

import io.firebolt.jdbc.client.FireboltClient;
import io.firebolt.jdbc.connection.settings.FireboltProperties;
import io.firebolt.jdbc.exception.FireboltException;
import lombok.RequiredArgsConstructor;
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
import java.util.ArrayList;
import java.util.List;

@RequiredArgsConstructor
@Slf4j
public class QueryClientImpl extends FireboltClient implements QueryClient {
  private final CloseableHttpClient httpClient;

  public InputStream postSqlQuery(
      String sql, String queryId, String accessToken, FireboltProperties fireboltProperties)
      throws FireboltException {
    HttpEntity requestEntity = null;
    try {
      List<NameValuePair> queryParameters = this.getQueryParameters(fireboltProperties, queryId);
      String uri = this.buildQueryUri(fireboltProperties, queryParameters).toString();
      requestEntity = new StringEntity(sql, StandardCharsets.UTF_8);
      HttpPost post = this.createPostRequest(uri, accessToken);
      post.setEntity(requestEntity);

      CloseableHttpResponse response = httpClient.execute(post);
      validateResponse(uri, response);
      return response.getEntity().getContent();
    } catch (Exception e) {
      EntityUtils.consumeQuietly(requestEntity);
      throw new FireboltException(
          String.format("Error executing query %s, error %s", sql, e.getMessage()), e);
    }
  }

  private List<NameValuePair> getQueryParameters(
      FireboltProperties fireboltProperties, String queryId) {
    List<NameValuePair> queryParams = new ArrayList<>();

    fireboltProperties
        .getAdditionalProperties()
        .forEach((k, v) -> queryParams.add(new BasicNameValuePair(k, v)));

    queryParams.add(new BasicNameValuePair("output_format", "TabSeparatedWithNamesAndTypes"));
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
}
