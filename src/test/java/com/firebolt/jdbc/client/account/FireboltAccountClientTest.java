package com.firebolt.jdbc.client.account;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.jdbc.VersionUtil;
import com.firebolt.jdbc.client.account.response.FireboltAccountResponse;
import com.firebolt.jdbc.client.account.response.FireboltDatabaseResponse;
import com.firebolt.jdbc.client.account.response.FireboltEngineIdResponse;
import com.firebolt.jdbc.client.account.response.FireboltEngineResponse;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.exception.FireboltException;
import com.google.common.collect.ImmutableMap;
import okhttp3.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junitpioneer.jupiter.SetSystemProperty;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.net.HttpURLConnection.*;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@SetSystemProperty(key = "java.version", value = "8.0.1")
@SetSystemProperty(key = "os.version", value = "10.1")
@SetSystemProperty(key = "os.name", value = "MacosX")
@ExtendWith(MockitoExtension.class)
class FireboltAccountClientTest {

    private static final String ACCESS_TOKEN = "token";
    private static final String HOST = "https://host";
    private static final String ACCOUNT = "account";
    private static final String ACCOUNT_ID = "account_id";
    private static final String DB_NAME = "dbName";
    private static final String ENGINE_NAME = "engineName";
    private static MockedStatic<VersionUtil> mockedProjectVersionUtil;

    @Spy
    private final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final ObjectMapper mapper = new ObjectMapper();
    @Captor
    private ArgumentCaptor<Request> requestArgumentCaptor;
    @Mock
    private OkHttpClient httpClient;
    @Mock
    private Call call;
    private FireboltAccountClient fireboltAccountClient;

    @Mock
    private FireboltConnection fireboltConnection;

    @BeforeAll
    static void init() {
        mockedProjectVersionUtil = mockStatic(VersionUtil.class);
        mockedProjectVersionUtil.when(VersionUtil::getDriverVersion).thenReturn("1.0-TEST");
    }

    @AfterAll
    public static void close() {
        mockedProjectVersionUtil.close();
    }

    @BeforeEach
    void setUp() throws FireboltException {
        fireboltAccountClient = new FireboltAccountClient(httpClient, objectMapper, fireboltConnection, "ConnA:1.0.9",
                "ConnB:2.0.9");
        when(httpClient.newCall(any())).thenReturn(call);
    }

    @Test
    void shouldGetAccountId() throws Exception {
        Response response = mock(Response.class);
        when(response.code()).thenReturn(HTTP_OK);
        ResponseBody body = mock(ResponseBody.class);
        when(body.string()).thenReturn("{\"account_id\":\"12345\"}");
        when(response.body()).thenReturn(body);
        when(call.execute()).thenReturn(response);

        Optional<String> accountId = fireboltAccountClient.getAccountId(HOST, ACCOUNT, ACCESS_TOKEN);

        Map<String, String> expectedHeader = ImmutableMap.of("User-Agent",
                "ConnB/2.0.9 JDBC/1.0-TEST (Java 8.0.1; Darwin 10.1; ) ConnA/1.0.9", "Authorization",
                "Bearer " + ACCESS_TOKEN);

        verify(httpClient).newCall(requestArgumentCaptor.capture());
        verify(objectMapper).readValue("{\"account_id\":\"12345\"}", FireboltAccountResponse.class);
        assertEquals("https://host/iam/v2/accounts:getIdByName?accountName=" + ACCOUNT, requestArgumentCaptor.getValue().url().toString());
        assertEquals(expectedHeader, extractHeadersMap(requestArgumentCaptor.getValue()));
        assertEquals("12345", accountId.get());
    }

    @Test
    void shouldGetEngineEndpoint() throws Exception {
        Response response = mock(Response.class);
        when(response.code()).thenReturn(HTTP_OK);
        ResponseBody body = mock(ResponseBody.class);
        when(response.body()).thenReturn(body);
        when(call.execute()).thenReturn(response);
        when(response.body().string()).thenReturn(mapper.writeValueAsString(FireboltEngineResponse
                .builder().engine(FireboltEngineResponse.Engine.builder().endpoint("http://engineEndpoint").build()).build()));
        when(httpClient.newCall(any())).thenReturn(call);

        String engineAddress = fireboltAccountClient.getEngineAddress(HOST, ENGINE_NAME, DB_NAME, ACCOUNT_ID,
                ACCESS_TOKEN);
        Map<String, String> expectedHeader = ImmutableMap.of("User-Agent",
                "ConnB/2.0.9 JDBC/1.0-TEST (Java 8.0.1; Darwin 10.1; ) ConnA/1.0.9", "Authorization",
                "Bearer " + ACCESS_TOKEN);

        verify(httpClient).newCall(requestArgumentCaptor.capture());
        verify(objectMapper).readValue("{\"engine\":{\"endpoint\":\"http://engineEndpoint\"}}",
                FireboltEngineResponse.class);
        verify(httpClient).newCall(requestArgumentCaptor.capture());
        assertEquals("https://host/core/v1/accounts/engineName/engines/" + ACCOUNT_ID, requestArgumentCaptor.getValue().url().toString());
        assertEquals(expectedHeader, extractHeadersMap(requestArgumentCaptor.getValue()));
        assertEquals("http://engineEndpoint", engineAddress);
    }

    @Test
    void shouldGetDbAddress() throws Exception {
        Response response = mock(Response.class);
        when(response.code()).thenReturn(HTTP_OK);
        ResponseBody body = mock(ResponseBody.class);
        when(response.body()).thenReturn(body);
        when(call.execute()).thenReturn(response);
        when(response.body().string()).thenReturn(mapper.writeValueAsString(FireboltDatabaseResponse.builder().engineUrl("http://dbAddress").build()));

        String dbAddress = fireboltAccountClient.getDbDefaultEngineAddress(HOST, ACCOUNT_ID, DB_NAME, ACCESS_TOKEN);
        Map<String, String> expectedHeader = ImmutableMap.of("User-Agent",
                "ConnB/2.0.9 JDBC/1.0-TEST (Java 8.0.1; Darwin 10.1; ) ConnA/1.0.9", "Authorization",
                "Bearer " + ACCESS_TOKEN);

        verify(httpClient).newCall(requestArgumentCaptor.capture());
        verify(objectMapper).readValue("{\"engine_url\":\"http://dbAddress\"}", FireboltDatabaseResponse.class);
        assertEquals(String.format("https://host/core/v1/accounts/%s/engines:getURLByDatabaseName?databaseName=%s", ACCOUNT_ID, DB_NAME), requestArgumentCaptor.getValue().url().toString());
        assertEquals(expectedHeader, extractHeadersMap(requestArgumentCaptor.getValue()));
        assertEquals("http://dbAddress", dbAddress);
    }

    @Test
    void shouldGetEngineId() throws Exception {
        Response response = mock(Response.class);
        when(response.code()).thenReturn(HTTP_OK);
        ResponseBody body = mock(ResponseBody.class);
        when(response.body()).thenReturn(body);
        when(call.execute()).thenReturn(response);
        when(response.body().string()).thenReturn(mapper.writeValueAsString(FireboltEngineIdResponse
                .builder().engine(FireboltEngineIdResponse.Engine.builder().engineId("13").build()).build()));

        String engineId = fireboltAccountClient.getEngineId(HOST, ACCOUNT_ID, ENGINE_NAME, ACCESS_TOKEN);
        Map<String, String> expectedHeader = ImmutableMap.of("User-Agent",
                "ConnB/2.0.9 JDBC/1.0-TEST (Java 8.0.1; Darwin 10.1; ) ConnA/1.0.9", "Authorization",
                "Bearer " + ACCESS_TOKEN);

        verify(httpClient).newCall(requestArgumentCaptor.capture());
        verify(objectMapper).readValue("{\"engine_id\":{\"engine_id\":\"13\"}}", FireboltEngineIdResponse.class);
        assertEquals(String.format(
                "https://host/core/v1/accounts/%s/engines:getIdByName?engine_name=%s", ACCOUNT_ID, ENGINE_NAME), requestArgumentCaptor.getValue().url().toString());
        assertEquals(expectedHeader, extractHeadersMap(requestArgumentCaptor.getValue()));
        assertEquals("13", engineId);
    }

    @Test
    void shouldThrowExceptionWhenStatusCodeIsNotFound() throws Exception {
        Response response = mock(Response.class);
        when(response.code()).thenReturn(HTTP_NOT_FOUND);
        ResponseBody body = mock(ResponseBody.class);
        when(response.body()).thenReturn(body);
        when(call.execute()).thenReturn(response);
        assertThrows(FireboltException.class, () -> fireboltAccountClient.getAccountId(HOST, ACCOUNT, ACCESS_TOKEN));
    }

    @Test
    void shouldThrowExceptionWhenStatusCodeIsNotOk() throws Exception {
        Response response = mock(Response.class);
        when(response.code()).thenReturn(HTTP_BAD_GATEWAY);
        ResponseBody body = mock(ResponseBody.class);
        when(response.body()).thenReturn(body);
        when(call.execute()).thenReturn(response);
        assertThrows(FireboltException.class, () -> fireboltAccountClient.getAccountId(HOST, ACCOUNT, ACCESS_TOKEN));
    }

    @Test
    void shouldThrowExceptionWithDBNotFoundErrorMessageWhenDBIsNotFound() throws Exception {
        Response response = mock(Response.class);
        when(response.code()).thenReturn(HTTP_NOT_FOUND);
        ResponseBody body = mock(ResponseBody.class);
        when(response.body()).thenReturn(body);
        when(call.execute()).thenReturn(response);
        assertThrows(FireboltException.class,
                () -> fireboltAccountClient.getDbDefaultEngineAddress(HOST, ACCOUNT, DB_NAME, ACCESS_TOKEN));
    }

	@Test
	void shouldThrowExceptionWithEngineNotFoundErrorMessageWhenEngineAddressIsNotFound() throws Exception {
		Response response = mock(Response.class);
		when(response.code()).thenReturn(HTTP_NOT_FOUND);
		ResponseBody body = mock(ResponseBody.class);
		when(response.body()).thenReturn(body);
		when(call.execute()).thenReturn(response);
		FireboltException fireboltException = assertThrows(FireboltException.class,
				() -> fireboltAccountClient.getEngineAddress(HOST, ACCOUNT, ENGINE_NAME, "123", ACCESS_TOKEN));
		assertEquals("The address of the engine with name engineName and id 123 could not be found",
				fireboltException.getMessage());
	}

	@Test
	void shouldThrowExceptionWithEngineNotFoundErrorMessageWhenEngineIdIsNotFound() throws Exception {
		Response response = mock(Response.class);
		when(response.code()).thenReturn(HTTP_NOT_FOUND);
		ResponseBody body = mock(ResponseBody.class);
		when(response.body()).thenReturn(body);
		when(call.execute()).thenReturn(response);
		FireboltException fireboltException = assertThrows(FireboltException.class,
				() -> fireboltAccountClient.getEngineId(HOST, ACCOUNT, ENGINE_NAME, ACCESS_TOKEN));
		assertEquals("The engine engineName could not be found", fireboltException.getMessage());
	}

    private Map<String, String> extractHeadersMap(Request request) {
		Map<String, String> headers = new HashMap<>();
		request.headers().forEach(header ->
				headers.put(header.getFirst(), header.getSecond()));
		return headers;
    }
}
