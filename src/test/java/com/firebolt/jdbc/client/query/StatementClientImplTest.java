package com.firebolt.jdbc.client.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.jdbc.client.authentication.FireboltAuthenticationClient;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.FireboltConnectionTokens;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.connection.settings.FireboltSessionProperty;
import com.firebolt.jdbc.exception.ExceptionType;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.service.FireboltAuthenticationService;
import com.firebolt.jdbc.service.FireboltStatementService;
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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;

import static com.firebolt.jdbc.client.UserAgentFormatter.userAgent;
import static com.firebolt.jdbc.client.query.StatementClientImpl.HEADER_UPDATE_PARAMETER;
import static java.lang.String.format;
import static java.util.Optional.ofNullable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StatementClientImplTest {
	private static final String HOST = "firebolt1";
	private static final FireboltProperties FIREBOLT_PROPERTIES = FireboltProperties.builder().database("db1").compress(true).host("firebolt1").port(555).build();
	@Captor
	private ArgumentCaptor<Request> requestArgumentCaptor;
	@Mock
	private OkHttpClient okHttpClient;

	@Mock
	private FireboltConnection connection;

	@ParameterizedTest
	@CsvSource({
			"false,http://firebolt1:555/?database=db1&output_format=TabSeparatedWithNamesAndTypes&compress=1&max_execution_time=15",
			"true,http://firebolt1:555/?database=db1&account_id=12345&output_format=TabSeparatedWithNamesAndTypes"
	})
	void shouldPostSqlQueryWithExpectedUrl(boolean systemEngine, String expectedUrl) throws FireboltException, IOException {
		assertEquals(expectedUrl, shouldPostSqlQuery(systemEngine).getValue());
	}

	private Entry<String, String> shouldPostSqlQuery(boolean systemEngine) throws FireboltException, IOException {
		FireboltProperties fireboltProperties = FireboltProperties.builder().database("db1").compress(true).host("firebolt1").port(555).accountId("12345").systemEngine(systemEngine).build();
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
		statementClient.abortStatement(id, FIREBOLT_PROPERTIES);
		verify(okHttpClient).newCall(requestArgumentCaptor.capture());
		assertEquals("http://firebolt1:555/cancel?query_id=12345",
				requestArgumentCaptor.getValue().url().uri().toString());
	}

	@Test
	void shouldIgnoreIfStatementIsNotFoundInDbWhenCancelSqlQuery() throws SQLException, IOException {
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
		statementClient.abortStatement(id, FIREBOLT_PROPERTIES);
		verify(okHttpClient).newCall(requestArgumentCaptor.capture());
		assertEquals("http://firebolt1:555/cancel?query_id=12345",
				requestArgumentCaptor.getValue().url().uri().toString());
	}

	@Test
	void cannotGetStatementIdWhenCancelling() throws SQLException {
		String id = "12345";
		StatementClient statementClient = new StatementClientImpl(okHttpClient, mock(ObjectMapper.class), connection, "", "");
		PreparedStatement ps = mock(PreparedStatement.class);
		ResultSet rs = mock(ResultSet.class);
		when(connection.prepareStatement(anyString())).thenReturn(ps);
		when(ps.executeQuery()).thenReturn(rs);
		when(rs.next()).thenReturn(true);
		when(rs.getString(1)).thenReturn(null);
		when(okHttpClient.dispatcher()).thenReturn(mock(Dispatcher.class));
		assertEquals("Cannot retrieve id for statement with label " + id, assertThrows(FireboltException.class, () -> statementClient.abortStatement(id, FIREBOLT_PROPERTIES)).getMessage());
	}

	@ParameterizedTest
	@CsvSource({
			"401,The operation is not authorized",
			"500, Server failed to execute query"
	})
	void shouldFailToCancelSqlQuery(int httpStatus, String errorMessage) throws SQLException, IOException {
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
		FireboltException e = assertThrows(FireboltException.class, () -> statementClient.abortStatement(id, FIREBOLT_PROPERTIES));
		assertTrue(e.getMessage().contains(errorMessage));
	}

	@Test
	void shouldRetryOnUnauthorized() throws IOException, SQLException {
		when(connection.getAccessToken()).thenReturn(Optional.of("oldToken")).thenReturn(Optional.of("newToken"));
		Call okCall = getMockedCallWithResponse(200, "");
		Call unauthorizedCall = getMockedCallWithResponse(401, "");
		when(okHttpClient.newCall(any())).thenReturn(unauthorizedCall).thenReturn(okCall);
		StatementClient statementClient = new StatementClientImpl(okHttpClient, mock(ObjectMapper.class), connection,
				"ConnA:1.0.9", "ConnB:2.0.9");
		StatementInfoWrapper statementInfoWrapper = StatementUtil.parseToStatementInfoWrappers("show databases").get(0);
		statementClient.executeSqlStatement(statementInfoWrapper, FIREBOLT_PROPERTIES, false, 5, true);
		verify(okHttpClient, times(2)).newCall(requestArgumentCaptor.capture());
		assertEquals("Bearer oldToken", requestArgumentCaptor.getAllValues().get(0).headers().get("Authorization")); // legit:ignore-secrets
		assertEquals("Bearer newToken", requestArgumentCaptor.getAllValues().get(1).headers().get("Authorization")); // legit:ignore-secrets
		verify(connection).removeExpiredTokens();
	}

	@Test
	void shouldNotRetryNoMoreThanOnceOnUnauthorized() throws IOException, FireboltException {
		Call okCall = getMockedCallWithResponse(200, "");
		Call unauthorizedCall = getMockedCallWithResponse(401, "");
		when(okHttpClient.newCall(any())).thenReturn(unauthorizedCall).thenReturn(unauthorizedCall).thenReturn(okCall);
		StatementClient statementClient = new StatementClientImpl(okHttpClient, mock(ObjectMapper.class), connection,
				"ConnA:1.0.9", "ConnB:2.0.9");
		StatementInfoWrapper statementInfoWrapper = StatementUtil.parseToStatementInfoWrappers("show databases").get(0);
		FireboltException ex = assertThrows(FireboltException.class, () -> statementClient.executeSqlStatement(statementInfoWrapper, FIREBOLT_PROPERTIES, false, 5, true));
		assertEquals(ExceptionType.UNAUTHORIZED, ex.getType());
		verify(okHttpClient, times(2)).newCall(any());
		verify(connection, times(2)).removeExpiredTokens();
	}

	@ParameterizedTest
	@CsvSource({
			"db1,db1,db1,db1",
			"db1,db2,db2,db2",
			"db1,db2,,db1", // no header returned
			"db1,db2,db3,db3" // use db2 but switched to db3
	})
	void use(String oldDb, String newDb, String responseHeaderDb, String expectedDb) throws IOException, SQLException {
		Map<String, List<String>> responseHeaders = responseHeaderDb == null ? null : Map.of(HEADER_UPDATE_PARAMETER, List.of("database=" + responseHeaderDb));
		try (FireboltConnection connection = use(oldDb,  "use " + newDb, responseHeaders)) {
			assertEquals(expectedDb, connection.getSessionProperties().getDatabase());
		}
	}

	// this function can be used for "use engine" when it is supported
	private FireboltConnection use(String oldDb, String useCommand, Map<String, List<String>> responseHeaders) throws IOException, SQLException {
		Properties props = new Properties();
		props.setProperty("database", oldDb);
		props.setProperty("compress", "true");
		props.setProperty("host", "firebolt1");
		props.setProperty("port", "555");
		props.setProperty(FireboltSessionProperty.CONNECTION_TIMEOUT_MILLIS.getKey(), "0"); // simplifies mocking
		FireboltProperties fireboltProperties = FireboltProperties.of(props);
		Call okCall = getMockedCallWithResponse(200, "", responseHeaders);
		when(okHttpClient.newCall(any())).thenReturn(okCall);
		FireboltAuthenticationService fireboltAuthenticationService = mock(FireboltAuthenticationService.class);
		FireboltConnectionTokens tokens = mock(FireboltConnectionTokens.class);
		when(fireboltAuthenticationService.getConnectionTokens(eq(format("https://%s:%d", fireboltProperties.getHost(), fireboltProperties.getPort())), any())).thenReturn(tokens);
		FireboltStatementService fireboltStatementService = mock(FireboltStatementService.class);
		lenient().when(fireboltStatementService.execute(any(), any(), anyInt(), anyInt(), anyBoolean(), anyBoolean(), any())).thenReturn(Optional.of(mock(ResultSet.class))); //SELECT 1 after setting property
		FireboltConnection connection = new FireboltConnection("url", props, fireboltAuthenticationService, fireboltStatementService, "0") {
			{
				try {
					connect();
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
			}
			@Override
			protected FireboltAuthenticationClient createFireboltAuthenticationClient(OkHttpClient httpClient, ObjectMapper objectMapper) {
				return null;
			}

			@Override
			protected void authenticate() {
				sessionProperties = loginProperties;
			}

			@Override
			protected void assertDatabaseExisting(String database) {

			}
		};
		StatementClient statementClient = new StatementClientImpl(okHttpClient, mock(ObjectMapper.class), connection,
				"ConnA:1.0.9", "ConnB:2.0.9");
		StatementInfoWrapper statementInfoWrapper = StatementUtil.parseToStatementInfoWrappers(useCommand).get(0);
		statementClient.executeSqlStatement(statementInfoWrapper, fireboltProperties, false, 5, true);
		return connection;
	}

	@ParameterizedTest
	@CsvSource(
			value = {
					"HTTP status code: 401 Unauthorized, body: {\"error\":\"Authentication token is invalid\",\"code\":16,\"message\":\"Authentication token is invalid\",\"details\":[{\"@type\":\"type.googleapis.com/google.rpc.DebugInfo\",\"stack_entries\":[],\"detail\":\"failed to get user_id from fawkes: entity not found\"}]}; Please associate user with your service account.",
					"Engine MyEngine does not exist or not authorized; Please grant at least one role to user associated your service account."
			},
			delimiter = ';')
	void shouldThrowUnauthorizedExceptionWhenNoAssociatedUser(String serverErrorMessage, String exceptionMessage) throws IOException, FireboltException {
		when(connection.getAccessToken()).thenReturn(Optional.of("token"));
		Call unauthorizedCall = getMockedCallWithResponse(500, serverErrorMessage);

		when(okHttpClient.newCall(any())).thenReturn(unauthorizedCall);
		StatementClient statementClient = new StatementClientImpl(okHttpClient, mock(ObjectMapper.class), connection,
				"ConnA:1.0.9", "ConnB:2.0.9");
		StatementInfoWrapper statementInfoWrapper = StatementUtil.parseToStatementInfoWrappers("show databases").get(0);
		FireboltException exception = assertThrows(FireboltException.class, () -> statementClient.executeSqlStatement(statementInfoWrapper, FIREBOLT_PROPERTIES, false, 5, true));
		assertEquals(ExceptionType.UNAUTHORIZED, exception.getType());
		assertEquals(format("Could not query Firebolt at %s. %s", HOST, exceptionMessage), exception.getMessage());
	}

	@ParameterizedTest
	@CsvSource({
			"java.io.IOException, ERROR",
			"okhttp3.internal.http2.StreamResetException, CANCELED",
			"java.lang.IllegalArgumentException, ERROR",
	})
	<T extends Exception> void shouldThrowIOException(Class<T> exceptionClass, ExceptionType exceptionType) throws IOException {
		Call call = mock(Call.class);
		when(call.execute()).thenThrow(exceptionClass);
		when(okHttpClient.newCall(any())).thenReturn(call);
		StatementClient statementClient = new StatementClientImpl(okHttpClient, mock(ObjectMapper.class), connection, "", "");
		StatementInfoWrapper statementInfoWrapper = StatementUtil.parseToStatementInfoWrappers("select 1").get(0);
		FireboltException ex = assertThrows(FireboltException.class, () -> statementClient.executeSqlStatement(statementInfoWrapper, FIREBOLT_PROPERTIES, false, 5, true));
		assertEquals(exceptionType, ex.getType());
		assertEquals(exceptionClass, ex.getCause().getClass());
	}

	private Call getMockedCallWithResponse(int statusCode, String content) throws IOException {
		return getMockedCallWithResponse(statusCode, content, Map.of());
	}

	private Call getMockedCallWithResponse(int statusCode, String content, Map<String, List<String>> responseHeaders) throws IOException {
		Call call = mock(Call.class);
		Response response = mock(Response.class);
		lenient().when(response.code()).thenReturn(statusCode);
		ofNullable(responseHeaders).ifPresent(headers -> headers.forEach((key, value) -> lenient().when(response.headers(key)).thenReturn(value)));
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