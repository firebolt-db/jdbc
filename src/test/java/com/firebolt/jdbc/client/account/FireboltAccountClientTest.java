package com.firebolt.jdbc.client.account;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.jdbc.client.account.response.FireboltAccountResponse;
import com.firebolt.jdbc.client.account.response.FireboltDefaultDatabaseEngineResponse;
import com.firebolt.jdbc.client.account.response.FireboltEngineIdResponse;
import com.firebolt.jdbc.client.account.response.FireboltEngineResponse;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.exception.FireboltException;
import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.junit.function.ThrowingRunnable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;

import static com.firebolt.jdbc.client.UserAgentFormatter.userAgent;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_BAD_GATEWAY;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FireboltAccountClientTest {
	private static final String ACCESS_TOKEN = "token";
	private static final String HOST = "https://host";
	private static final String ACCOUNT = "account";
	private static final String ACCOUNT_ID = "account_id";
	private static final String DB_NAME = "dbName";
	private static final String ENGINE_NAME = "engineName";

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

	@BeforeEach
	void setUp() {
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

		FireboltAccountResponse account = fireboltAccountClient.getAccount(HOST, ACCOUNT, ACCESS_TOKEN);

		Map<String, String> expectedHeader = Map.of("User-Agent",
				userAgent("ConnB/2.0.9 JDBC/%s (Java %s; %s %s; ) ConnA/1.0.9"), "Authorization",
				"Bearer " + ACCESS_TOKEN);

		verify(httpClient).newCall(requestArgumentCaptor.capture());
		verify(objectMapper).readValue("{\"account_id\":\"12345\"}", FireboltAccountResponse.class);
		assertEquals("https://host/iam/v2/accounts:getIdByName?accountName=" + ACCOUNT,
				requestArgumentCaptor.getValue().url().toString());
		assertEquals(expectedHeader, extractHeadersMap(requestArgumentCaptor.getValue()));
		assertEquals("12345", account.getAccountId());
	}

	@Test
	void shouldGetEngineEndpoint() throws Exception {
		Response response = mock(Response.class);
		when(response.code()).thenReturn(HTTP_OK);
		ResponseBody body = mock(ResponseBody.class);
		when(response.body()).thenReturn(body);
		when(call.execute()).thenReturn(response);
		when(response.body().string()).thenReturn(mapper.writeValueAsString(FireboltEngineResponse.builder()
				.engine(FireboltEngineResponse.Engine.builder().endpoint("http://engineEndpoint").build()).build()));
		when(httpClient.newCall(any())).thenReturn(call);

		FireboltEngineResponse engine = fireboltAccountClient.getEngine(HOST, ENGINE_NAME, DB_NAME, ACCOUNT_ID,
				ACCESS_TOKEN);
		Map<String, String> expectedHeader = Map.of("User-Agent",
				userAgent("ConnB/2.0.9 JDBC/%s (Java %s; %s %s; ) ConnA/1.0.9"), "Authorization",
				"Bearer " + ACCESS_TOKEN);

		verify(httpClient).newCall(requestArgumentCaptor.capture());
		verify(objectMapper).readValue("{\"engine\":{\"endpoint\":\"http://engineEndpoint\",\"current_status\":null}}",
				FireboltEngineResponse.class);
		verify(httpClient).newCall(requestArgumentCaptor.capture());
		assertEquals("https://host/core/v1/accounts/engineName/engines/" + ACCOUNT_ID,
				requestArgumentCaptor.getValue().url().toString());
		assertEquals(expectedHeader, extractHeadersMap(requestArgumentCaptor.getValue()));
		assertEquals("http://engineEndpoint", engine.getEngine().getEndpoint());
	}

	@Test
	void shouldGetDbAddress() throws Exception {
		Response response = mock(Response.class);
		when(response.code()).thenReturn(HTTP_OK);
		ResponseBody body = mock(ResponseBody.class);
		when(response.body()).thenReturn(body);
		when(call.execute()).thenReturn(response);
		when(response.body().string()).thenReturn(mapper
				.writeValueAsString(FireboltDefaultDatabaseEngineResponse.builder().engineUrl("http://dbAddress").build()));

		FireboltDefaultDatabaseEngineResponse fireboltDefaultDatabaseEngineResponse = fireboltAccountClient
				.getDefaultEngineByDatabaseName(HOST, ACCOUNT_ID, DB_NAME, ACCESS_TOKEN);
		Map<String, String> expectedHeader = Map.of("User-Agent",
				userAgent("ConnB/2.0.9 JDBC/%s (Java %s; %s %s; ) ConnA/1.0.9"), "Authorization",
				"Bearer " + ACCESS_TOKEN);

		verify(httpClient).newCall(requestArgumentCaptor.capture());
		verify(objectMapper).readValue("{\"engine_url\":\"http://dbAddress\"}", FireboltDefaultDatabaseEngineResponse.class);
		assertEquals(format("https://host/core/v1/accounts/%s/engines:getURLByDatabaseName?databaseName=%s",
				ACCOUNT_ID, DB_NAME), requestArgumentCaptor.getValue().url().toString());
		assertEquals(expectedHeader, extractHeadersMap(requestArgumentCaptor.getValue()));
		assertEquals("http://dbAddress", fireboltDefaultDatabaseEngineResponse.getEngineUrl());
	}

	@Test
	void shouldGetEngineId() throws Exception {
		Response response = mock(Response.class);
		when(response.code()).thenReturn(HTTP_OK);
		ResponseBody body = mock(ResponseBody.class);
		when(response.body()).thenReturn(body);
		when(call.execute()).thenReturn(response);
		when(response.body().string()).thenReturn(mapper.writeValueAsString(FireboltEngineIdResponse.builder()
				.engine(FireboltEngineIdResponse.Engine.builder().engineId("13").build()).build()));

		FireboltEngineIdResponse fireboltEngineIdResponse = fireboltAccountClient.getEngineId(HOST, ACCOUNT_ID,
				ENGINE_NAME, ACCESS_TOKEN);
		Map<String, String> expectedHeader = Map.of("User-Agent",
				userAgent("ConnB/2.0.9 JDBC/%s (Java %s; %s %s; ) ConnA/1.0.9"), "Authorization",
				"Bearer " + ACCESS_TOKEN);

		verify(httpClient).newCall(requestArgumentCaptor.capture());
		verify(objectMapper).readValue("{\"engine_id\":{\"engine_id\":\"13\"}}", FireboltEngineIdResponse.class);
		assertEquals(format("https://host/core/v1/accounts/%s/engines:getIdByName?engine_name=%s", ACCOUNT_ID,
				ENGINE_NAME), requestArgumentCaptor.getValue().url().toString());
		assertEquals(expectedHeader, extractHeadersMap(requestArgumentCaptor.getValue()));
		assertEquals("13", fireboltEngineIdResponse.getEngine().getEngineId());
	}

	@Test
	void shouldThrowExceptionWhenStatusCodeIsNotFound() throws Exception {
		shouldThrowException(HTTP_NOT_FOUND, () -> fireboltAccountClient.getAccount(HOST, ACCOUNT, ACCESS_TOKEN), null);
	}

	@Test
	void shouldThrowExceptionWhenStatusCodeIsNotOk() throws Exception {
		shouldThrowException(HTTP_BAD_GATEWAY, () -> fireboltAccountClient.getAccount(HOST, ACCOUNT, ACCESS_TOKEN), null);
	}

	@Test
	void shouldThrowExceptionWithDBNotFoundErrorMessageWhenDBIsNotFound() throws Exception {
		shouldThrowException(HTTP_NOT_FOUND, () -> fireboltAccountClient.getDefaultEngineByDatabaseName(HOST, ACCOUNT, DB_NAME, ACCESS_TOKEN), "The database with the name dbName could not be found");
	}

	@Test
	void shouldThrowExceptionWithEngineNotFoundErrorMessageWhenEngineAddressIsNotFound() throws Exception {
		shouldThrowException(HTTP_NOT_FOUND, () -> fireboltAccountClient.getEngine(HOST, ACCOUNT, ENGINE_NAME, "123", ACCESS_TOKEN), "The address of the engine with name engineName and id 123 could not be found");
	}

	@Test
	void shouldThrowExceptionWithEngineNotFoundErrorMessageWhenEngineIdIsNotFound() throws Exception {
		shouldThrowException(HTTP_NOT_FOUND, () -> fireboltAccountClient.getEngineId(HOST, ACCOUNT, ENGINE_NAME, ACCESS_TOKEN), "The engine engineName could not be found");
	}

	private void shouldThrowException(int httpStatus, ThrowingRunnable runnable, String expectedMessage) throws Exception {
		Response response = mock(Response.class);
		when(response.code()).thenReturn(httpStatus);
		ResponseBody body = mock(ResponseBody.class);
		when(response.body()).thenReturn(body);
		when(call.execute()).thenReturn(response);
		FireboltException fireboltException = assertThrows(FireboltException.class, runnable);
		if (expectedMessage != null) {
			assertEquals(expectedMessage, fireboltException.getMessage());
		}
	}

	private Map<String, String> extractHeadersMap(Request request) {
		Map<String, String> headers = new HashMap<>();
		request.headers().forEach(header -> headers.put(header.getFirst(), header.getSecond()));
		return headers;
	}
}
