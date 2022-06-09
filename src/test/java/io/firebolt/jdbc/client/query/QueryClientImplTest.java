package io.firebolt.jdbc.client.query;

import io.firebolt.jdbc.connection.settings.FireboltProperties;
import io.firebolt.jdbc.exception.FireboltException;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class QueryClientImplTest {

  @Mock private CloseableHttpClient closeableHttpClient;

  @Captor ArgumentCaptor<HttpPost> httpPostArgumentCaptor;

  @Test
  void shouldPostSqlQueryWithExpectedUrl()
      throws FireboltException, IOException, URISyntaxException {
    FireboltProperties fireboltProperties =
        FireboltProperties.builder().database("db1").compress(1).host("firebolt1").port(80).build();
    String accessToken = "token";
    QueryClient queryClient = new QueryClientImpl(closeableHttpClient);
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    HttpEntity httpEntity = mock(HttpEntity.class);
    when(response.getCode()).thenReturn(200);
    when(response.getEntity()).thenReturn(httpEntity);
    when(closeableHttpClient.execute(any())).thenReturn(response);

    queryClient.postSqlQuery("show databases", true, "123456", accessToken, fireboltProperties);

    verify(closeableHttpClient).execute(httpPostArgumentCaptor.capture());
    HttpPost actualHttpPost = httpPostArgumentCaptor.getValue();
    Map<String, String> expectedHeaders = new HashMap();
    expectedHeaders.put("Authorization", "Bearer token");
    expectedHeaders.put("User-Agent", "fireboltJdbcDriver/2.5-SNAPSHOT");
    String actualQuery =
        new BufferedReader(
                new InputStreamReader(
                    actualHttpPost.getEntity().getContent(), StandardCharsets.UTF_8))
            .lines()
            .collect(Collectors.joining("\n"));

    assertEquals(
        "http://firebolt1:80/?output_format=TabSeparatedWithNamesAndTypes&database=db1&query_id=123456&compress=1",
        actualHttpPost.getUri().toString());
    assertEquals(expectedHeaders, extractHeadersMap(actualHttpPost));
    assertEquals("show databases", actualQuery);
  }

  @Test
  void shouldCancelSqlQuery() throws FireboltException, IOException, URISyntaxException {
    FireboltProperties fireboltProperties =
        FireboltProperties.builder().database("db1").compress(1).host("firebolt1").port(80).build();
    QueryClient queryClient = new QueryClientImpl(closeableHttpClient);

    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    when(response.getCode()).thenReturn(200);
    when(closeableHttpClient.execute(any())).thenReturn(response);
    queryClient.postCancelSqlQuery("12345", fireboltProperties);
    HttpPost httpPost = new HttpPost("http://firebolt1:80/cancel?query_id=12345");
    verify(closeableHttpClient).execute(httpPostArgumentCaptor.capture());
    assertEquals(httpPost.getUri(), httpPostArgumentCaptor.getValue().getUri());
  }

  private Map<String, String> extractHeadersMap(HttpPost httpPost) {
    return Arrays.stream(httpPost.getHeaders())
        .collect(Collectors.toMap(Header::getName, Header::getValue));
  }
}
