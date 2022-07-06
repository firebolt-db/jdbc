package io.firebolt.jdbc.client.account;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.firebolt.jdbc.ProjectVersionUtil;
import io.firebolt.jdbc.client.account.response.FireboltAccountResponse;
import io.firebolt.jdbc.client.account.response.FireboltDatabaseResponse;
import io.firebolt.jdbc.client.account.response.FireboltEngineIdResponse;
import io.firebolt.jdbc.client.account.response.FireboltEngineResponse;
import io.firebolt.jdbc.connection.FireboltConnection;
import io.firebolt.jdbc.connection.FireboltConnectionTokens;
import io.firebolt.jdbc.exception.FireboltException;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpStatus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FireboltAccountClientTest {

  private static final String ACCESS_TOKEN = "token";
  private static final String HOST = "https://host";
  private static final String ACCOUNT = "account";
  private static final String ACCOUNT_ID = "account_id";
  private static final String DB_NAME = "dbName";
  private static final String ENGINE_NAME = "engineName";
  private static MockedStatic<ProjectVersionUtil> mockedProjectVersionUtil;

  @Spy
  private final ObjectMapper objectMapper =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  private final ObjectMapper mapper = new ObjectMapper();
  @Captor ArgumentCaptor<HttpGet> httpGetArgumentCaptor;
  @Mock private CloseableHttpClient httpClient;
  private FireboltAccountClient fireboltAccountClient;

  @Mock private FireboltConnection fireboltConnection;

  @BeforeAll
  static void init() {
    mockedProjectVersionUtil = mockStatic(ProjectVersionUtil.class);
    mockedProjectVersionUtil.when(ProjectVersionUtil::getProjectVersion).thenReturn("1.0-TEST");
    System.setProperty("java.version", "8.0.1");
    System.setProperty("os.version", "10.1");
    System.setProperty("os.name", "MacosX");
  }

  @AfterAll
  public static void close() {
    mockedProjectVersionUtil.close();
    System.clearProperty("java.version");
    System.clearProperty("os.version");
    System.clearProperty("os.name");
  }

  @BeforeEach
  void setUp() throws FireboltException {
    when(fireboltConnection.getConnectionTokens()).thenReturn(Optional.empty());
    when(fireboltConnection.getConnectionTokens())
        .thenReturn(
            Optional.of(FireboltConnectionTokens.builder().accessToken(ACCESS_TOKEN).build()));
    fireboltAccountClient =
        new FireboltAccountClient(
            httpClient, objectMapper, fireboltConnection, "ConnA:1.0.9", "ConnB:2.0.9");
  }

  @Test
  void shouldGetAccountId() throws Exception {
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    when(response.getCode()).thenReturn(HttpStatus.SC_OK);
    HttpEntity entity = mock(HttpEntity.class);
    when(entity.getContent())
        .thenReturn(
            new ByteArrayInputStream(
                mapper.writeValueAsBytes(
                    FireboltAccountResponse.builder().accountId("12345").build())));
    when(response.getEntity()).thenReturn(entity);
    when(httpClient.execute(any())).thenReturn(response);

    Optional<String> accountId = fireboltAccountClient.getAccountId(HOST, ACCOUNT);

    HttpGet expectedHttpGet =
        new HttpGet("https://host/iam/v2/accounts:getIdByName?accountName=" + ACCOUNT);
    Map<String, String> expectedHeader =
        ImmutableMap.of(
            "User-Agent",
            "ConnB/2.0.9 JDBC/1.0-TEST (Java 8.0.1; Darwin 10.1; ) ConnA/1.0.9",
            "Authorization",
            "Bearer " + ACCESS_TOKEN);

    verify(httpClient).execute(httpGetArgumentCaptor.capture());
    verify(objectMapper).readValue("{\"account_id\":\"12345\"}", FireboltAccountResponse.class);
    HttpGet actualHttpGet = httpGetArgumentCaptor.getValue();
    assertEquals(expectedHttpGet.getUri(), httpGetArgumentCaptor.getValue().getUri());
    assertEquals(expectedHeader, extractHeadersMap(actualHttpGet));
    assertEquals("12345", accountId.get());
  }

  @Test
  void shouldGetEngineEndpoint() throws Exception {
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    HttpEntity entity = mock(HttpEntity.class);
    when(response.getCode()).thenReturn(HttpStatus.SC_OK);
    when(entity.getContent())
        .thenReturn(
            new ByteArrayInputStream(
                mapper.writeValueAsBytes(
                    FireboltEngineResponse.builder()
                        .engine(
                            FireboltEngineResponse.Engine.builder()
                                .endpoint("http://engineEndpoint")
                                .build())
                        .build())));
    when(response.getEntity()).thenReturn(entity);
    when(httpClient.execute(any())).thenReturn(response);

    String engineAddress =
        fireboltAccountClient.getEngineAddress(HOST, ENGINE_NAME, DB_NAME, ACCOUNT_ID);
    HttpGet expectedHttpGet =
        new HttpGet("https://host/core/v1/accounts/engineName/engines/" + ACCOUNT_ID);
    Map<String, String> expectedHeader =
        ImmutableMap.of(
            "User-Agent",
            "ConnB/2.0.9 JDBC/1.0-TEST (Java 8.0.1; Darwin 10.1; ) ConnA/1.0.9",
            "Authorization",
            "Bearer " + ACCESS_TOKEN);

    verify(httpClient).execute(httpGetArgumentCaptor.capture());
    verify(objectMapper)
        .readValue(
            "{\"engine\":{\"endpoint\":\"http://engineEndpoint\"}}", FireboltEngineResponse.class);
    HttpGet actualHttpGet = httpGetArgumentCaptor.getValue();
    assertEquals(expectedHttpGet.getUri(), httpGetArgumentCaptor.getValue().getUri());
    assertEquals(expectedHeader, extractHeadersMap(actualHttpGet));
    assertEquals("http://engineEndpoint", engineAddress);
  }

  @Test
  void shouldGetDbAddress() throws Exception {
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    HttpEntity entity = mock(HttpEntity.class);
    when(response.getCode()).thenReturn(HttpStatus.SC_OK);
    when(entity.getContent())
        .thenReturn(
            new ByteArrayInputStream(
                mapper.writeValueAsBytes(
                    FireboltDatabaseResponse.builder().engineUrl("http://dbAddress").build())));
    when(response.getEntity()).thenReturn(entity);
    when(httpClient.execute(any())).thenReturn(response);

    String dbAddress = fireboltAccountClient.getDbDefaultEngineAddress(HOST, ACCOUNT_ID, DB_NAME);
    HttpGet expectedHttpGet =
        new HttpGet(
            String.format(
                "https://host/core/v1/accounts/%s/engines:getURLByDatabaseName?databaseName=%s",
                ACCOUNT_ID, DB_NAME));
    Map<String, String> expectedHeader =
        ImmutableMap.of(
            "User-Agent",
            "ConnB/2.0.9 JDBC/1.0-TEST (Java 8.0.1; Darwin 10.1; ) ConnA/1.0.9",
            "Authorization",
            "Bearer " + ACCESS_TOKEN);

    verify(httpClient).execute(httpGetArgumentCaptor.capture());
    verify(objectMapper)
        .readValue("{\"engine_url\":\"http://dbAddress\"}", FireboltDatabaseResponse.class);
    HttpGet actualHttpGet = httpGetArgumentCaptor.getValue();
    assertEquals(expectedHttpGet.getUri(), httpGetArgumentCaptor.getValue().getUri());
    assertEquals(expectedHeader, extractHeadersMap(actualHttpGet));
    assertEquals("http://dbAddress", dbAddress);
  }

  @Test
  void shouldGetEngineId() throws Exception {
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    HttpEntity entity = mock(HttpEntity.class);
    when(response.getCode()).thenReturn(HttpStatus.SC_OK);
    when(entity.getContent())
        .thenReturn(
            new ByteArrayInputStream(
                mapper.writeValueAsBytes(
                    FireboltEngineIdResponse.builder()
                        .engine(FireboltEngineIdResponse.Engine.builder().engineId("13").build())
                        .build())));
    when(response.getEntity()).thenReturn(entity);
    when(httpClient.execute(any())).thenReturn(response);

    String engineId = fireboltAccountClient.getEngineId(HOST, ACCOUNT_ID, ENGINE_NAME);
    HttpGet expectedHttpGet =
        new HttpGet(
            String.format(
                "https://host/core/v1/accounts/%s/engines:getIdByName?engine_name=%s",
                ACCOUNT_ID, ENGINE_NAME));
    Map<String, String> expectedHeader =
        ImmutableMap.of(
            "User-Agent",
            "ConnB/2.0.9 JDBC/1.0-TEST (Java 8.0.1; Darwin 10.1; ) ConnA/1.0.9",
            "Authorization",
            "Bearer " + ACCESS_TOKEN);

    verify(httpClient).execute(httpGetArgumentCaptor.capture());
    verify(objectMapper)
        .readValue("{\"engine_id\":{\"engine_id\":\"13\"}}", FireboltEngineIdResponse.class);
    HttpGet actualHttpGet = httpGetArgumentCaptor.getValue();
    assertEquals(expectedHttpGet.getUri(), httpGetArgumentCaptor.getValue().getUri());
    assertEquals(expectedHeader, extractHeadersMap(actualHttpGet));
    assertEquals("13", engineId);
  }

  @Test
  void shouldThrowExceptionWhenStatusCodeIsNotFound() throws Exception {
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    when(response.getCode()).thenReturn(HttpStatus.SC_NOT_FOUND);
    HttpEntity entity = mock(HttpEntity.class);
    when(response.getEntity()).thenReturn(entity);
    when(httpClient.execute(any())).thenReturn(response);
    assertThrows(FireboltException.class, () -> fireboltAccountClient.getAccountId(HOST, ACCOUNT));
  }

  @Test
  void shouldThrowExceptionWhenStatusCodeIsNotOk() throws Exception {
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    when(response.getCode()).thenReturn(HttpStatus.SC_BAD_GATEWAY);
    HttpEntity entity = mock(HttpEntity.class);
    when(response.getEntity()).thenReturn(entity);
    when(httpClient.execute(any())).thenReturn(response);

    assertThrows(FireboltException.class, () -> fireboltAccountClient.getAccountId(HOST, ACCOUNT));
  }

  private Map<String, String> extractHeadersMap(HttpGet httpGet) {
    return Arrays.stream(httpGet.getHeaders())
        .collect(Collectors.toMap(Header::getName, Header::getValue));
  }
}
