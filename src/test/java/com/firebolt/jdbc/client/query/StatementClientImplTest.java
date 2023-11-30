package com.firebolt.jdbc.client.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.ExceptionType;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.statement.StatementInfoWrapper;
import com.firebolt.jdbc.statement.StatementUtil;
import lombok.NonNull;
import okhttp3.Call;
import okhttp3.Dispatcher;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.Buffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import static com.firebolt.jdbc.client.UserAgentFormatter.userAgent;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StatementClientImplTest {
	@Captor
	private ArgumentCaptor<Request> requestArgumentCaptor;
	@Mock
	private OkHttpClient okHttpClient;

	@Mock
	private FireboltConnection connection;

	@Test
	void shouldPostSqlQueryWithExpectedUrl() throws FireboltException, IOException {
		Entry<String, String> result = shouldPostSqlQueryForSystemEngine(false);
		String requestId = result.getKey();
		String url = result.getValue();
		assertEquals(
				"http://firebolt1:555/?database=db1&output_format=TabSeparatedWithNamesAndTypes&compress=1&max_execution_time=15",
				//format("http://firebolt1:555/?database=db1&output_format=TabSeparatedWithNamesAndTypes&compress=1&query_label=%s&max_execution_time=15", requestId),
				url);
	}

	@Test
	void shouldPostSqlQueryForSystemEngine() throws FireboltException, IOException {
		String url = shouldPostSqlQueryForSystemEngine(true).getValue();
		assertEquals(
				"http://firebolt1:555/?database=db1&account_id=12345&output_format=TabSeparatedWithNamesAndTypes",
				url);
	}

	private Entry<String, String> shouldPostSqlQueryForSystemEngine(boolean systemEngine) throws FireboltException, IOException {
		FireboltProperties fireboltProperties = FireboltProperties.builder().database("db1").compress(true)
				.host("firebolt1").port(555).accountId("12345").systemEngine(systemEngine).build();
		when(connection.getAccessToken())
				.thenReturn(Optional.of("token"));
		StatementClient statementClient = new StatementClientImpl(okHttpClient, mock(ObjectMapper.class), connection,
				"ConnA:1.0.9", "ConnB:2.0.9");
		injectMockedResponse(okHttpClient, 200, "");
		Call call = getMockedCallWithResponse(200, "");
		when(okHttpClient.newCall(any())).thenReturn(call);
		StatementInfoWrapper statementInfoWrapper = StatementUtil.parseToStatementInfoWrappers("show databases").get(0);
		statementClient.executeSqlStatement(statementInfoWrapper, fireboltProperties, fireboltProperties.isSystemEngine(), 15, true);

		verify(okHttpClient).newCall(requestArgumentCaptor.capture());
		Request actualRequest = requestArgumentCaptor.getValue();
		String actualQuery = getActualRequestString(actualRequest);
		Map<String, String> expectedHeaders = new LinkedHashMap<>();
		expectedHeaders.put("Authorization", "Bearer token");
		expectedHeaders.put("User-Agent", userAgent("ConnB/2.0.9 JDBC/%s (Java %s; %s %s; ) ConnA/1.0.9"));
		assertEquals(expectedHeaders, extractHeadersMap(actualRequest));
		//assertEquals("show databases;", actualQuery);
		assertSqlStatement("show databases;", actualQuery);
		return Map.entry(statementInfoWrapper.getLabel(), actualRequest.url().toString());
	}

	@Test
	void shouldCancelSqlQuery() throws SQLException, IOException {
		FireboltProperties fireboltProperties = FireboltProperties.builder().database("db1").compress(true)
				.host("firebolt1").port(555).build();
		String id = "12345";
		StatementClient statementClient = new StatementClientImpl(okHttpClient, mock(ObjectMapper.class), connection, "", "");
		PreparedStatement ps = mock(PreparedStatement.class);
		ResultSet rs = mock(ResultSet.class);
		when(connection.prepareStatement(anyString())).thenReturn(ps);
		when(ps.executeQuery()).thenReturn(rs);
		when(rs.next()).thenReturn(true);
		when(rs.getString(1)).thenReturn(id);
		injectMockedResponse(okHttpClient, 200, "");
		Call call = getMockedCallWithResponse(200, "");
		when(okHttpClient.newCall(any())).thenReturn(call);
		when(okHttpClient.dispatcher()).thenReturn(mock(Dispatcher.class));
		statementClient.abortStatement(id, fireboltProperties);
		verify(okHttpClient).newCall(requestArgumentCaptor.capture());
		assertEquals("http://firebolt1:555/cancel?query_id=12345",
				requestArgumentCaptor.getValue().url().uri().toString());
	}

	@Test
	void shouldIgnoreIfStatementIsNotFoundInDbWhenCancelSqlQuery() throws SQLException, IOException {
		FireboltProperties fireboltProperties = FireboltProperties.builder().database("db1").compress(true)
				.host("firebolt1").port(555).build();
		String id = "12345";
		StatementClient statementClient = new StatementClientImpl(okHttpClient, mock(ObjectMapper.class), connection, "", "");
		PreparedStatement ps = mock(PreparedStatement.class);
		ResultSet rs = mock(ResultSet.class);
		when(connection.prepareStatement(anyString())).thenReturn(ps);
		when(ps.executeQuery()).thenReturn(rs);
		when(rs.next()).thenReturn(true);
		when(rs.getString(1)).thenReturn(id);
		injectMockedResponse(okHttpClient, 200, "");
		Call call = getMockedCallWithResponse(400, ""); // BAD REQUEST
		when(okHttpClient.newCall(any())).thenReturn(call);
		when(okHttpClient.dispatcher()).thenReturn(mock(Dispatcher.class));
		statementClient.abortStatement(id, fireboltProperties);
		verify(okHttpClient).newCall(requestArgumentCaptor.capture());
		assertEquals("http://firebolt1:555/cancel?query_id=12345",
				requestArgumentCaptor.getValue().url().uri().toString());
	}

	@Test
	void cannotGetStatementIdWhenCancelling() throws SQLException, IOException {
		FireboltProperties fireboltProperties = FireboltProperties.builder().database("db1").compress(true)
				.host("firebolt1").port(555).build();
		String id = "12345";
		StatementClient statementClient = new StatementClientImpl(okHttpClient, mock(ObjectMapper.class), connection, "", "");
		PreparedStatement ps = mock(PreparedStatement.class);
		ResultSet rs = mock(ResultSet.class);
		when(connection.prepareStatement(anyString())).thenReturn(ps);
		when(ps.executeQuery()).thenReturn(rs);
		when(rs.next()).thenReturn(true);
		when(rs.getString(1)).thenReturn(null);
		when(okHttpClient.dispatcher()).thenReturn(mock(Dispatcher.class));
		assertEquals("Cannot retrieve id for statement with label " + id, assertThrows(FireboltException.class, () -> statementClient.abortStatement(id, fireboltProperties)).getMessage());
	}

	@ParameterizedTest
	@CsvSource({
			"401,The operation is not authorized",
			"500, Server failed to execute query"
	})
	void shouldFailToCancelSqlQuery(int httpStatus, String errorMessage) throws SQLException, IOException {
		FireboltProperties fireboltProperties = FireboltProperties.builder().database("db1").compress(true)
				.host("firebolt1").port(555).build();
		String id = "12345";
		StatementClient statementClient = new StatementClientImpl(okHttpClient, mock(ObjectMapper.class), connection, "", "");
		PreparedStatement ps = mock(PreparedStatement.class);
		ResultSet rs = mock(ResultSet.class);
		when(connection.prepareStatement(anyString())).thenReturn(ps);
		when(ps.executeQuery()).thenReturn(rs);
		when(rs.next()).thenReturn(true);
		when(rs.getString(1)).thenReturn(id);
		Call call = getMockedCallWithResponse(httpStatus, "");
		when(okHttpClient.newCall(any())).thenReturn(call);
		when(okHttpClient.dispatcher()).thenReturn(mock(Dispatcher.class));
		FireboltException e = assertThrows(FireboltException.class, () -> statementClient.abortStatement(id, fireboltProperties));
		assertTrue(e.getMessage().contains(errorMessage));
	}

	@Test
	void shouldRetryOnUnauthorized() throws IOException, SQLException {
		FireboltProperties fireboltProperties = FireboltProperties.builder().database("db1").compress(true)
				.host("firebolt1").port(555).build();
		when(connection.getAccessToken()).thenReturn(Optional.of("oldToken")).thenReturn(Optional.of("newToken"));
		Call okCall = getMockedCallWithResponse(200, "");
		Call unauthorizedCall = getMockedCallWithResponse(401, "");
		when(okHttpClient.newCall(any())).thenReturn(unauthorizedCall).thenReturn(okCall);
		StatementClient statementClient = new StatementClientImpl(okHttpClient, mock(ObjectMapper.class), connection,
				"ConnA:1.0.9", "ConnB:2.0.9");
		StatementInfoWrapper statementInfoWrapper = StatementUtil.parseToStatementInfoWrappers("show databases").get(0);
		statementClient.executeSqlStatement(statementInfoWrapper, fireboltProperties, false, 5, true);
		verify(okHttpClient, times(2)).newCall(requestArgumentCaptor.capture());
		assertEquals("Bearer oldToken" ,requestArgumentCaptor.getAllValues().get(0).headers().get("Authorization"));
		assertEquals("Bearer newToken" ,requestArgumentCaptor.getAllValues().get(1).headers().get("Authorization"));
		verify(connection).removeExpiredTokens();
	}

	@Test
	void shouldNotRetryNoMoreThanOnceOnUnauthorized() throws IOException, FireboltException {
		FireboltProperties fireboltProperties = FireboltProperties.builder().database("db1").compress(true)
				.host("firebolt1").port(555).build();
		Call okCall = getMockedCallWithResponse(200, "");
		Call unauthorizedCall = getMockedCallWithResponse(401, "");
		when(okHttpClient.newCall(any())).thenReturn(unauthorizedCall).thenReturn(unauthorizedCall).thenReturn(okCall);
		StatementClient statementClient = new StatementClientImpl(okHttpClient, mock(ObjectMapper.class), connection,
				"ConnA:1.0.9", "ConnB:2.0.9");
		StatementInfoWrapper statementInfoWrapper = StatementUtil.parseToStatementInfoWrappers("show databases").get(0);
		FireboltException ex = assertThrows(FireboltException.class, () -> statementClient.executeSqlStatement(statementInfoWrapper, fireboltProperties, false, 5, true));
		assertEquals(ExceptionType.UNAUTHORIZED, ex.getType());
		verify(okHttpClient, times(2)).newCall(any());
		verify(connection, times(2)).removeExpiredTokens();
	}

	@ParameterizedTest
	@CsvSource(
			value = {
					"HTTP status code: 401 Unauthorized, body: {\"error\":\"Authentication token is invalid\",\"code\":16,\"message\":\"Authentication token is invalid\",\"details\":[{\"@type\":\"type.googleapis.com/google.rpc.DebugInfo\",\"stack_entries\":[],\"detail\":\"failed to get user_id from fawkes: entity not found\"}]}; Please associate user with your service account.",
					"Engine MyEngine does not exist or not authorized; Please grant at least one role to user associated your service account."
			},
			delimiter = ';')
	void shouldThrowUnauthorizedExceptionWhenNoAssociatedUser(String serverErrorMessage, String exceptionMessage) throws IOException, FireboltException {
		String host = "firebolt1";
		FireboltProperties fireboltProperties = FireboltProperties.builder().database("db1").compress(true)
				.host(host).port(555).build();
		when(connection.getAccessToken()).thenReturn(Optional.of("token"));
		Call unauthorizedCall = getMockedCallWithResponse(500, serverErrorMessage);

		when(okHttpClient.newCall(any())).thenReturn(unauthorizedCall);
		StatementClient statementClient = new StatementClientImpl(okHttpClient, mock(ObjectMapper.class), connection,
				"ConnA:1.0.9", "ConnB:2.0.9");
		StatementInfoWrapper statementInfoWrapper = StatementUtil.parseToStatementInfoWrappers("show databases").get(0);
		FireboltException exception = assertThrows(FireboltException.class, () -> statementClient.executeSqlStatement(statementInfoWrapper, fireboltProperties, false, 5, true));
		assertEquals(ExceptionType.UNAUTHORIZED, exception.getType());
		assertEquals(format("Could not query Firebolt at %s. %s", host, exceptionMessage), exception.getMessage());
	}

	@ParameterizedTest
	@CsvSource({
			"java.io.IOException, ERROR",
			"okhttp3.internal.http2.StreamResetException, CANCELED",
			"java.lang.IllegalArgumentException, ERROR",
	})
	<T extends Exception> void shouldThrowIOException(Class<T> exceptionClass, ExceptionType exceptionType) throws IOException {
		FireboltProperties fireboltProperties = FireboltProperties.builder().database("db1").compress(true)
				.host("firebolt1").port(555).build();
		Call call = mock(Call.class);
		when(call.execute()).thenThrow(exceptionClass);
		when(okHttpClient.newCall(any())).thenReturn(call);
		StatementClient statementClient = new StatementClientImpl(okHttpClient, mock(ObjectMapper.class), connection, "", "");
		StatementInfoWrapper statementInfoWrapper = StatementUtil.parseToStatementInfoWrappers("select 1").get(0);
		FireboltException ex = assertThrows(FireboltException.class, () -> statementClient.executeSqlStatement(statementInfoWrapper, fireboltProperties, false, 5, true));
		assertEquals(exceptionType, ex.getType());
		assertEquals(exceptionClass, ex.getCause().getClass());
	}

	private Call getMockedCallWithResponse(int statusCode, String content) throws IOException {
		Call call = mock(Call.class);
		Response response = mock(Response.class);
		lenient().when(response.code()).thenReturn(statusCode);
		ResponseBody body = mock(ResponseBody.class);
		lenient().when(response.body()).thenReturn(body);
		lenient().when(body.bytes()).thenReturn(content.getBytes());
		lenient().when(call.execute()).thenReturn(response);
		return call;
	}

	private Map<String, String> extractHeadersMap(Request request) {
		Map<String, String> headers = new HashMap<>();
		request.headers().forEach(header -> headers.put(header.getFirst(), header.getSecond()));
		return headers;
	}

	private void injectMockedResponse(OkHttpClient httpClient, int code, String content) throws IOException {
		Response response = mock(Response.class);
		Call call = mock(Call.class);
		lenient().when(httpClient.newCall(any())).thenReturn(call);
		lenient().when(call.execute()).thenReturn(response);
		ResponseBody body = mock(ResponseBody.class);
		lenient().when(response.body()).thenReturn(body);
		lenient().when(body.bytes()).thenReturn(content.getBytes());
		lenient().when(response.code()).thenReturn(code);
	}

	@NonNull
	private String getActualRequestString(Request actualRequest) throws IOException {
		Buffer buffer = new Buffer();
		actualRequest.body().writeTo(buffer);
		return buffer.readUtf8();
	}

	private void assertSqlStatement(String expected, String actual) {
		assertTrue(actual.matches(expected + "--label:[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}"));
	}
}