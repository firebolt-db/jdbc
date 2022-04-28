package io.firebolt.jdbc.client.account;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.firebolt.jdbc.ProjectVersionUtil;
import io.firebolt.jdbc.client.account.response.FireboltAccountResponse;
import io.firebolt.jdbc.client.account.response.FireboltDatabaseResponse;
import io.firebolt.jdbc.client.account.response.FireboltEngineIdResponse;
import io.firebolt.jdbc.client.account.response.FireboltEngineResponse;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.HttpVersion;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicStatusLine;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.IOException;
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

    private final static String ACCESS_TOKEN = "token";
    private final static String HOST = "https://host";
    private final static String ACCOUNT = "account";
    private final static String ACCOUNT_ID = "account_id";
    private final static String DB_NAME = "dbName";
    private final static String ENGINE_NAME = "engineName";
    private static MockedStatic<ProjectVersionUtil> mockedProjectVersionUtil;
    @Spy
    private final ObjectMapper objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private final ObjectMapper mapper = new ObjectMapper();
    @Captor
    ArgumentCaptor<HttpGet> httpGetArgumentCaptor;
    @Mock
    private CloseableHttpClient httpClient;
    private FireboltAccountClient fireboltAccountClient;

    @BeforeAll
    static void init() {
        mockedProjectVersionUtil = mockStatic(ProjectVersionUtil.class);
        mockedProjectVersionUtil.when(ProjectVersionUtil::getProjectVersion).thenReturn("1.0-TEST");
    }

    @AfterAll
    public static void close() {
        mockedProjectVersionUtil.close();
    }

    @BeforeEach
    void setUp() {
        fireboltAccountClient = new FireboltAccountClient(httpClient, objectMapper);
    }

    @Test
    void shouldGetAccountId() throws Exception {
        CloseableHttpResponse response = mock(CloseableHttpResponse.class);
        when(response.getStatusLine()).thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, HttpStatus.SC_OK, "OK"));
        HttpEntity entity = mock(HttpEntity.class);
        when(entity.getContent()).thenReturn(new ByteArrayInputStream(mapper.writeValueAsBytes(FireboltAccountResponse.builder().accountId("12345").build())));
        when(response.getEntity()).thenReturn(entity);
        when(httpClient.execute(any())).thenReturn(response);


        Optional<String> accountId = fireboltAccountClient.getAccountId(HOST, ACCOUNT, ACCESS_TOKEN);

        HttpGet expectedHttpGet = new HttpGet("https://host/iam/v2/accounts:getIdByName?accountName=" + ACCOUNT);
        Map<String, String> expectedHeader = ImmutableMap.of("User-Agent", "fireboltJdbcDriver/1.0-TEST", "Authorization", "Bearer " + ACCESS_TOKEN);

        verify(httpClient).execute(httpGetArgumentCaptor.capture());
        verify(objectMapper).readValue("{\"account_id\":\"12345\"}", FireboltAccountResponse.class);
        HttpGet actualHttpGet = httpGetArgumentCaptor.getValue();
        assertEquals(expectedHttpGet.getURI(), httpGetArgumentCaptor.getValue().getURI());
        assertEquals(expectedHeader, headersToMap(actualHttpGet));
        assertEquals("12345", accountId.get());
    }

    @Test
    void shouldGetEngineEndpoint() throws Exception {
        CloseableHttpResponse response = mock(CloseableHttpResponse.class);
        HttpEntity entity = mock(HttpEntity.class);
        when(response.getStatusLine()).thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, HttpStatus.SC_OK, "OK"));
        when(entity.getContent()).thenReturn(new ByteArrayInputStream(mapper.writeValueAsBytes(FireboltEngineResponse.builder().engine(FireboltEngineResponse.Engine.builder().endpoint("http://engineEndpoint").build()).build())));
        when(response.getEntity()).thenReturn(entity);
        when(httpClient.execute(any())).thenReturn(response);

        String engineAddress = fireboltAccountClient.getEngineAddress(HOST, ENGINE_NAME, DB_NAME, ACCOUNT_ID, ACCESS_TOKEN);
        HttpGet expectedHttpGet = new HttpGet("https://host/core/v1/accounts/engineName/engines/" + ACCOUNT_ID);
        Map<String, String> expectedHeader = ImmutableMap.of("User-Agent", "fireboltJdbcDriver/1.0-TEST", "Authorization", "Bearer " + ACCESS_TOKEN);

        verify(httpClient).execute(httpGetArgumentCaptor.capture());
        verify(objectMapper).readValue("{\"engine\":{\"endpoint\":\"http://engineEndpoint\"}}", FireboltEngineResponse.class);
        HttpGet actualHttpGet = httpGetArgumentCaptor.getValue();
        assertEquals(expectedHttpGet.getURI(), httpGetArgumentCaptor.getValue().getURI());
        assertEquals(expectedHeader, headersToMap(actualHttpGet));
        assertEquals("http://engineEndpoint", engineAddress);
    }

    @Test
    void shouldGetDbAddress() throws Exception {
        CloseableHttpResponse response = mock(CloseableHttpResponse.class);
        HttpEntity entity = mock(HttpEntity.class);
        when(response.getStatusLine()).thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, HttpStatus.SC_OK, "OK"));
        when(entity.getContent()).thenReturn(new ByteArrayInputStream(mapper.writeValueAsBytes(FireboltDatabaseResponse.builder().engineUrl("http://dbAddress").build())));
        when(response.getEntity()).thenReturn(entity);
        when(httpClient.execute(any())).thenReturn(response);

        String dbAddress = fireboltAccountClient.getDbDefaultEngineAddress(HOST, ACCOUNT_ID, DB_NAME, ACCESS_TOKEN);
        HttpGet expectedHttpGet = new HttpGet(String.format("https://host/core/v1/accounts/%s/engines:getURLByDatabaseName?databaseName=%s", ACCOUNT_ID, DB_NAME));
        Map<String, String> expectedHeader = ImmutableMap.of("User-Agent", "fireboltJdbcDriver/1.0-TEST", "Authorization", "Bearer " + ACCESS_TOKEN);

        verify(httpClient).execute(httpGetArgumentCaptor.capture());
        verify(objectMapper).readValue("{\"engine_url\":\"http://dbAddress\"}", FireboltDatabaseResponse.class);
        HttpGet actualHttpGet = httpGetArgumentCaptor.getValue();
        assertEquals(expectedHttpGet.getURI(), httpGetArgumentCaptor.getValue().getURI());
        assertEquals(expectedHeader, headersToMap(actualHttpGet));
        assertEquals("http://dbAddress", dbAddress);
    }

    @Test
    void shouldGetEngineId() throws Exception {
        CloseableHttpResponse response = mock(CloseableHttpResponse.class);
        HttpEntity entity = mock(HttpEntity.class);
        when(response.getStatusLine()).thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, HttpStatus.SC_OK, "OK"));
        when(entity.getContent()).thenReturn(new ByteArrayInputStream(mapper.writeValueAsBytes(FireboltEngineIdResponse.builder().engine(FireboltEngineIdResponse.Engine.builder().engineId("13").build()).build())));
        when(response.getEntity()).thenReturn(entity);
        when(httpClient.execute(any())).thenReturn(response);

        String engineId = fireboltAccountClient.getEngineId(HOST, ACCOUNT_ID, ENGINE_NAME, ACCESS_TOKEN);
        HttpGet expectedHttpGet = new HttpGet(String.format("https://host/core/v1/accounts/%s/engines:getIdByName?engine_name=%s", ACCOUNT_ID, ENGINE_NAME));
        Map<String, String> expectedHeader = ImmutableMap.of("User-Agent", "fireboltJdbcDriver/1.0-TEST", "Authorization", "Bearer " + ACCESS_TOKEN);

        verify(httpClient).execute(httpGetArgumentCaptor.capture());
        verify(objectMapper).readValue("{\"engine_id\":{\"engine_id\":\"13\"}}", FireboltEngineIdResponse.class);
        HttpGet actualHttpGet = httpGetArgumentCaptor.getValue();
        assertEquals(expectedHttpGet.getURI(), httpGetArgumentCaptor.getValue().getURI());
        assertEquals(expectedHeader, headersToMap(actualHttpGet));
        assertEquals("13", engineId);
    }

    @Test
    void shouldThrowExceptionWhenStatusCodeIsNotFound() throws Exception {
        CloseableHttpResponse response = mock(CloseableHttpResponse.class);
        when(response.getStatusLine()).thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, HttpStatus.SC_NOT_FOUND, "NOT FOUND"));
        HttpEntity entity = mock(HttpEntity.class);
        when(response.getEntity()).thenReturn(entity);
        when(httpClient.execute(any())).thenReturn(response);
        assertThrows(IOException.class, () -> fireboltAccountClient.getAccountId(HOST, ACCOUNT, ACCESS_TOKEN));

    }

    @Test
    void shouldThrowExceptionWhenStatusCodeIsNotOk() throws Exception {
        CloseableHttpResponse response = mock(CloseableHttpResponse.class);
        when(response.getStatusLine()).thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, HttpStatus.SC_BAD_GATEWAY, "Bad Gateway"));
        HttpEntity entity = mock(HttpEntity.class);
        when(response.getEntity()).thenReturn(entity);
        when(httpClient.execute(any())).thenReturn(response);

        assertThrows(IOException.class, () -> fireboltAccountClient.getAccountId(HOST, ACCOUNT, ACCESS_TOKEN));

    }

    private Map<String, String> headersToMap(HttpGet httpGet) {
        return Arrays.stream(httpGet.getAllHeaders()).collect(Collectors.toMap(Header::getName, Header::getValue));
    }

}