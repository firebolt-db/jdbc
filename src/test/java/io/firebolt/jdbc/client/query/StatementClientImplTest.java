package io.firebolt.jdbc.client.query;

import com.google.common.collect.ImmutableMap;
import io.firebolt.jdbc.VersionUtil;
import io.firebolt.jdbc.connection.FireboltConnection;
import io.firebolt.jdbc.connection.FireboltConnectionTokens;
import io.firebolt.jdbc.connection.settings.FireboltProperties;
import io.firebolt.jdbc.exception.FireboltException;
import io.firebolt.jdbc.statement.StatementInfoWrapper;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junitpioneer.jupiter.SetSystemProperty;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.firebolt.jdbc.statement.StatementInfoWrapper.StatementType.QUERY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@SetSystemProperty(key = "java.version", value = "8.0.1")
@SetSystemProperty(key = "os.version", value = "10.1")
@SetSystemProperty(key = "os.name", value = "MacosX")
@ExtendWith(MockitoExtension.class)
class StatementClientImplTest {

  private static MockedStatic<VersionUtil> mockedProjectVersionUtil;

  @Mock private CloseableHttpClient closeableHttpClient;

  @Captor ArgumentCaptor<HttpPost> httpPostArgumentCaptor;

  @BeforeAll
  static void init() {
    mockedProjectVersionUtil = mockStatic(VersionUtil.class);
    mockedProjectVersionUtil.when(VersionUtil::getDriverVersion).thenReturn("1.0-TEST");
  }

  @AfterAll
  static void close() {
    mockedProjectVersionUtil.close();
  }

  @Test
  void shouldPostSqlQueryWithExpectedUrl()
      throws FireboltException, IOException, URISyntaxException {
    FireboltProperties fireboltProperties =
        FireboltProperties.builder().database("db1").compress(true).host("firebolt1").port(80).build();
    FireboltConnection connection = mock(FireboltConnection.class);
    when(connection.getConnectionTokens())
        .thenReturn(Optional.of(FireboltConnectionTokens.builder().accessToken("token").build()));
    StatementClient statementClient =
        new StatementClientImpl(closeableHttpClient, connection, "ConnA:1.0.9", "ConnB:2.0.9");
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    HttpEntity httpEntity = mock(HttpEntity.class);
    when(response.getCode()).thenReturn(200);
    when(response.getEntity()).thenReturn(httpEntity);
    when(closeableHttpClient.execute(any())).thenReturn(response);

    statementClient.postSqlStatement(
        StatementInfoWrapper.builder().sql("show databases").id("123456").type(QUERY).build(),
        fireboltProperties,
        ImmutableMap.of(
            "output_format",
            "TabSeparatedWithNamesAndTypes",
            "database",
            "db1",
            "query_id",
            "123456",
            "compress",
            "1"));

    verify(closeableHttpClient).execute(httpPostArgumentCaptor.capture());
    HttpPost actualHttpPost = httpPostArgumentCaptor.getValue();
    Map<String, String> expectedHeaders = new LinkedHashMap<>();
    expectedHeaders.put("Authorization", "Bearer token");
    expectedHeaders.put(
        "User-Agent", "ConnB/2.0.9 JDBC/1.0-TEST (Java 8.0.1; Darwin 10.1; ) ConnA/1.0.9");
    String actualQuery =
        new BufferedReader(
                new InputStreamReader(
                    actualHttpPost.getEntity().getContent(), StandardCharsets.UTF_8))
            .lines()
            .collect(Collectors.joining("\n"));

    assertEquals(expectedHeaders, extractHeadersMap(actualHttpPost));
    assertEquals("show databases;", actualQuery);
    assertEquals(
        "http://firebolt1:80/?output_format=TabSeparatedWithNamesAndTypes&database=db1&query_id=123456&compress=1",
        actualHttpPost.getUri().toString());
  }

  @Test
  void shouldCancelSqlQuery() throws FireboltException, IOException, URISyntaxException {
    FireboltProperties fireboltProperties =
        FireboltProperties.builder().database("db1").compress(true).host("firebolt1").port(80).build();
    StatementClient statementClient =
        new StatementClientImpl(closeableHttpClient, mock(FireboltConnection.class), "", "");

    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    when(response.getCode()).thenReturn(200);
    when(closeableHttpClient.execute(any())).thenReturn(response);
    statementClient.abortStatement(
        "12345", fireboltProperties, ImmutableMap.of("query_id", "12345"));
    HttpPost httpPost = new HttpPost("http://firebolt1:80/cancel?query_id=12345");
    verify(closeableHttpClient).execute(httpPostArgumentCaptor.capture());
    assertEquals(httpPost.getUri(), httpPostArgumentCaptor.getValue().getUri());
  }

  private Map<String, String> extractHeadersMap(HttpPost httpPost) {
    return Arrays.stream(httpPost.getHeaders())
        .collect(Collectors.toMap(Header::getName, Header::getValue));
  }
}
