package com.firebolt.jdbc.client.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

import com.firebolt.jdbc.exception.ExceptionType;
import org.jetbrains.annotations.NotNull;
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.jdbc.util.VersionUtil;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.statement.StatementInfoWrapper;
import com.firebolt.jdbc.statement.StatementUtil;

import okhttp3.*;
import okio.Buffer;

@SetSystemProperty(key = "java.version", value = "8.0.1")
@SetSystemProperty(key = "os.version", value = "10.1")
@SetSystemProperty(key = "os.name", value = "MacosX")
@ExtendWith(MockitoExtension.class)
class StatementClientImplTest {

	private static MockedStatic<VersionUtil> mockedProjectVersionUtil;
	@Captor
	private ArgumentCaptor<Request> requestArgumentCaptor;
	@Mock
	private OkHttpClient okHttpClient;

	@Mock
	private FireboltConnection connection;

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
	void shouldPostSqlQueryWithExpectedUrl() throws FireboltException, IOException {
		FireboltProperties fireboltProperties = FireboltProperties.builder().database("db1").compress(true)
				.host("firebolt1").port(555).build();
		when(connection.getAccessToken())
				.thenReturn(Optional.of("token"));
		StatementClient statementClient = new StatementClientImpl(okHttpClient, connection, mock(ObjectMapper.class),
				"ConnA:1.0.9", "ConnB:2.0.9");
		Call call = getMockedCallWithResponse(200);
		when(okHttpClient.newCall(any())).thenReturn(call);
		StatementInfoWrapper statementInfoWrapper = StatementUtil.parseToStatementInfoWrappers("show databases").get(0);
		statementClient.postSqlStatement(statementInfoWrapper, fireboltProperties, false, 15, 1, true);

		verify(okHttpClient).newCall(requestArgumentCaptor.capture());
		Request actualRequest = requestArgumentCaptor.getValue();
		String actualQuery = getActualRequestString(actualRequest);
		Map<String, String> expectedHeaders = new LinkedHashMap<>();
		expectedHeaders.put("Authorization", "Bearer token");
		expectedHeaders.put("User-Agent", "ConnB/2.0.9 JDBC/1.0-TEST (Java 8.0.1; Darwin 10.1; ) ConnA/1.0.9");

		assertEquals(expectedHeaders, extractHeadersMap(actualRequest));
		assertEquals("show databases;", actualQuery);
		assertEquals(String.format(
				"http://firebolt1:555/?result_overflow_mode=break&database=db1&output_format=TabSeparatedWithNamesAndTypes&query_id=%s&compress=1&max_result_rows=1&max_execution_time=15",
				statementInfoWrapper.getId()), actualRequest.url().toString());
	}

	@Test
	void shouldPostSqlQueryForSystemEngine() throws FireboltException, IOException, URISyntaxException {
		FireboltProperties fireboltProperties = FireboltProperties.builder().database("db1").compress(true)
				.host("firebolt1").port(555).build();
		when(connection.getAccessToken())
				.thenReturn(Optional.of("token"));
		StatementClient statementClient = new StatementClientImpl(okHttpClient, connection, mock(ObjectMapper.class),
				"ConnA:1.0.9", "ConnB:2.0.9");
		Call call = getMockedCallWithResponse(200);
		when(okHttpClient.newCall(any())).thenReturn(call);
		StatementInfoWrapper statementInfoWrapper = StatementUtil.parseToStatementInfoWrappers("show databases").get(0);
		statementClient.postSqlStatement(statementInfoWrapper, fireboltProperties, true, 15, 1, true);

		verify(okHttpClient).newCall(requestArgumentCaptor.capture());
		Request actualRequest = requestArgumentCaptor.getValue();
		String actualQuery = getActualRequestString(actualRequest);

		assertEquals("show databases;", actualQuery);
		assertEquals("http://firebolt1:555/?output_format=TabSeparatedWithNamesAndTypes",
				actualRequest.url().toString());
	}

	@Test
	void shouldPostSqlQueryForNonStandardSql() throws FireboltException, IOException {
		FireboltProperties fireboltProperties = FireboltProperties.builder().database("db1").compress(true)
				.host("firebolt1").port(555).build();
		when(connection.getAccessToken())
				.thenReturn(Optional.of("token"));
		StatementClient statementClient = new StatementClientImpl(okHttpClient, connection, mock(ObjectMapper.class),
				"ConnA:1.0.9", "ConnB:2.0.9");
		Call call = getMockedCallWithResponse(200);
		when(okHttpClient.newCall(any())).thenReturn(call);
		StatementInfoWrapper statementInfoWrapper = StatementUtil.parseToStatementInfoWrappers("show databases").get(0);
		statementClient.postSqlStatement(statementInfoWrapper, fireboltProperties, true, 15, 1, false);

		verify(okHttpClient).newCall(requestArgumentCaptor.capture());
		Request actualRequest = requestArgumentCaptor.getValue();
		String actualQuery = getActualRequestString(actualRequest);

		assertEquals("show databases;", actualQuery);
		assertEquals("http://firebolt1:555/?output_format=TabSeparatedWithNamesAndTypes&use_standard_sql=0",
				actualRequest.url().toString());
	}

	@Test
	void shouldCancelSqlQuery() throws FireboltException, IOException {
		FireboltProperties fireboltProperties = FireboltProperties.builder().database("db1").compress(true)
				.host("firebolt1").port(555).build();
		StatementClient statementClient = new StatementClientImpl(okHttpClient, connection,
				mock(ObjectMapper.class), "", "");
		Call call = getMockedCallWithResponse(200);
		when(okHttpClient.newCall(any())).thenReturn(call);
		statementClient.abortStatement("12345", fireboltProperties);
		verify(okHttpClient).newCall(requestArgumentCaptor.capture());
		assertEquals("http://firebolt1:555/cancel?query_id=12345",
				requestArgumentCaptor.getValue().url().uri().toString());
	}

	@Test
	void shouldRetryOnUnauthorized() throws IOException, FireboltException {
		FireboltProperties fireboltProperties = FireboltProperties.builder().database("db1").compress(true)
				.host("firebolt1").port(555).build();
		when(connection.getAccessToken()).thenReturn(Optional.of("oldToken"))
				.thenReturn(Optional.of("newToken"));
		Call okCall = getMockedCallWithResponse(200);
		Call unauthorizedCall = getMockedCallWithResponse(401);
		when(okHttpClient.newCall(any())).thenReturn(unauthorizedCall).thenReturn(okCall);
		StatementClient statementClient = new StatementClientImpl(okHttpClient, connection, mock(ObjectMapper.class),
				"ConnA:1.0.9", "ConnB:2.0.9");
		StatementInfoWrapper statementInfoWrapper = StatementUtil.parseToStatementInfoWrappers("show databases").get(0);
		statementClient.postSqlStatement(statementInfoWrapper, fireboltProperties, false, 5, 5, true);
		verify(okHttpClient, times(2)).newCall(requestArgumentCaptor.capture());
		assertEquals("Bearer oldToken" ,requestArgumentCaptor.getAllValues().get(0).headers().get("Authorization"));
		assertEquals("Bearer newToken" ,requestArgumentCaptor.getAllValues().get(1).headers().get("Authorization"));
		verify(connection).removeExpiredTokens();
	}

	@Test
	void shouldNotRetryNoMoreThanOnceOnUnauthorized() throws IOException, FireboltException {
		FireboltProperties fireboltProperties = FireboltProperties.builder().database("db1").compress(true)
				.host("firebolt1").port(555).build();
		Call okCall = getMockedCallWithResponse(200);
		Call unauthorizedCall = getMockedCallWithResponse(401);
		when(okHttpClient.newCall(any())).thenReturn(unauthorizedCall).thenReturn(unauthorizedCall).thenReturn(okCall);
		StatementClient statementClient = new StatementClientImpl(okHttpClient, connection, mock(ObjectMapper.class),
				"ConnA:1.0.9", "ConnB:2.0.9");
		StatementInfoWrapper statementInfoWrapper = StatementUtil.parseToStatementInfoWrappers("show databases").get(0);
		FireboltException ex = assertThrows(FireboltException.class, () -> statementClient.postSqlStatement(statementInfoWrapper, fireboltProperties, false, 5, 5, true));
		assertEquals(ExceptionType.UNAUTHORIZED, ex.getType());
		verify(okHttpClient, times(2)).newCall(any());
		verify(connection, times(2)).removeExpiredTokens();
	}

	private Call getMockedCallWithResponse(int statusCode) throws IOException {
		Call call = mock(Call.class);
		Response response = mock(Response.class);
		lenient().when(response.code()).thenReturn(statusCode);
		lenient().when(call.execute()).thenReturn(response);
		return call;
	}

	private Map<String, String> extractHeadersMap(Request request) {
		Map<String, String> headers = new HashMap<>();
		request.headers().forEach(header -> headers.put(header.getFirst(), header.getSecond()));
		return headers;
	}

	@NotNull
	private String getActualRequestString(Request actualRequest) throws IOException {
		Buffer buffer = new Buffer();
		actualRequest.body().writeTo(buffer);
		return buffer.readUtf8();
	}
}