package com.firebolt.jdbc.client.query;

import static com.firebolt.jdbc.client.UserAgentFormatter.userAgent;
import static com.firebolt.jdbc.client.query.StatementClientImpl.*;
import static java.lang.String.format;
import static java.util.Optional.ofNullable;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;

import com.firebolt.jdbc.client.authentication.FireboltAuthenticationClient;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.FireboltConnectionTokens;
import com.firebolt.jdbc.connection.UrlUtil;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.connection.settings.FireboltSessionProperty;
import com.firebolt.jdbc.exception.ExceptionType;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.statement.StatementInfoWrapper;
import com.firebolt.jdbc.statement.StatementUtil;
import com.firebolt.jdbc.type.ParserVersion;

import lombok.NonNull;
import okhttp3.*;
import okio.Buffer;

@ExtendWith(MockitoExtension.class)
class StatementClientImplTest {
	private static final String HOST = "firebolt1";
	private static final FireboltProperties FIREBOLT_PROPERTIES = FireboltProperties.builder().database("db1").compress(true).host("firebolt1").port(555).build();

	private static final int QUERY_TIMEOUT = 15;

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
	void shouldPostSqlQueryWithExpectedUrl(boolean systemEngine, String expectedUrl) throws SQLException, IOException {
		assertEquals(expectedUrl, shouldPostSqlQuery(systemEngine).getValue());
	}

	private Entry<String, String> shouldPostSqlQuery(boolean systemEngine) throws SQLException, IOException {
		FireboltProperties fireboltProperties = FireboltProperties.builder().database("db1").compress(true).host("firebolt1").port(555).accountId("12345").systemEngine(systemEngine).build();
		when(connection.getAccessToken())
				.thenReturn(Optional.of("token"));
		StatementClient statementClient = new StatementClientImpl(okHttpClient, connection, "ConnA:1.0.9", "ConnB:2.0.9");
		injectMockedResponse(okHttpClient, 200, "");
		Call call = getMockedCallWithResponse(200, "");
		when(okHttpClient.newCall(any())).thenReturn(call);
		StatementInfoWrapper statementInfoWrapper = StatementUtil.parseToStatementInfoWrappers("show databases").get(0);
		statementClient.executeSqlStatement(statementInfoWrapper, fireboltProperties, fireboltProperties.isSystemEngine(), 15);

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

	@ParameterizedTest
	@CsvSource({
			"true,2,queryLabelFromConnection",
			"true,2,null"
	})
	void shouldPostSqlWithExpectedQueryLabel(boolean systemEngine, int infraVersion, String connectionQueryLabel) throws SQLException, IOException {
		FireboltProperties fireboltProperties = FireboltProperties.builder().database("db1").compress(true).host("firebolt1").port(555).accountId("12345").systemEngine(systemEngine)
						.runtimeAdditionalProperties(Map.of("query_label", connectionQueryLabel)).build();
		when(connection.getAccessToken()).thenReturn(Optional.of("token"));
		when(connection.getInfraVersion()).thenReturn(infraVersion);

		StatementClient statementClient = new StatementClientImpl(okHttpClient, connection, "ConnA:1.0.9", "ConnB:2.0.9");
		injectMockedResponse(okHttpClient, 200, "");
		Call call = getMockedCallWithResponse(200, "");
		when(okHttpClient.newCall(any())).thenReturn(call);
		StatementInfoWrapper statementInfoWrapper = StatementUtil.parseToStatementInfoWrappers("show databases").get(0);
		assertNotNull(statementInfoWrapper.getLabel());
		statementClient.executeSqlStatement(statementInfoWrapper, fireboltProperties, fireboltProperties.isSystemEngine(), QUERY_TIMEOUT);

		verify(okHttpClient).newCall(requestArgumentCaptor.capture());
		Request actualRequest = requestArgumentCaptor.getValue();
		Map<String, String> expectedHeaders = new LinkedHashMap<>();
		expectedHeaders.put("Authorization", "Bearer token");
		expectedHeaders.put("User-Agent", userAgent("ConnB/2.0.9 JDBC/%s (Java %s; %s %s; ) ConnA/1.0.9"));
		assertEquals(expectedHeaders, extractHeadersMap(actualRequest));

		String actualQuery = getActualRequestString(actualRequest);
		assertEquals("show databases;", actualQuery);

		HttpUrl httpUrl = actualRequest.url();
		Set<String> queryParameterNames= httpUrl.queryParameterNames();
		assertTrue(queryParameterNames.contains("query_label"));

		String expectedConnectionQueryLabel = StringUtils.isNotBlank(connectionQueryLabel) ? connectionQueryLabel : statementInfoWrapper.getLabel();
		assertEquals(httpUrl.queryParameter("query_label"), expectedConnectionQueryLabel);
	}

	@ParameterizedTest(name = "infra version:{0}")
	@ValueSource(ints = {0, 1, 2})
	void shouldCancelSqlQuery(int infraVersion) throws SQLException, IOException {
		when(connection.getInfraVersion()).thenReturn(infraVersion);
		String id = "12345";
		StatementClient statementClient = new StatementClientImpl(okHttpClient, connection, "", "");
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

	@ParameterizedTest(name = "infra version:{0}")
	@ValueSource(ints = {0, 1, 2})
	void shouldIgnoreIfStatementIsNotFoundInDbWhenCancelSqlQuery(int infraVersion) throws SQLException, IOException {
		when(connection.getInfraVersion()).thenReturn(infraVersion);
		String id = "12345";
		StatementClient statementClient = new StatementClientImpl(okHttpClient, connection, "", "");
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

	@ParameterizedTest(name = "infra version:{0}")
	@ValueSource(ints = {0, 1, 2})
	void cannotGetStatementIdWhenCancelling(int infraVersion) throws SQLException {
		when(connection.getInfraVersion()).thenReturn(infraVersion);
		String id = "12345";
		StatementClient statementClient = new StatementClientImpl(okHttpClient, connection, "", "");
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
			"1,401,The operation is not authorized",
			"1,500, Server failed to execute query",
			"2,401,The operation is not authorized",
			"2,500, Server failed to execute query"
	})
	void shouldFailToCancelSqlQuery(int infraVersion, int httpStatus, String errorMessage) throws SQLException, IOException {
		when(connection.getInfraVersion()).thenReturn(infraVersion);
		String id = "12345";
		StatementClient statementClient = new StatementClientImpl(okHttpClient, connection, "", "");
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
		StatementClient statementClient = new StatementClientImpl(okHttpClient, connection, "ConnA:1.0.9", "ConnB:2.0.9");
		StatementInfoWrapper statementInfoWrapper = StatementUtil.parseToStatementInfoWrappers("show databases").get(0);
		statementClient.executeSqlStatement(statementInfoWrapper, FIREBOLT_PROPERTIES, false, 5);
		verify(okHttpClient, times(2)).newCall(requestArgumentCaptor.capture());
		assertEquals("Bearer oldToken", requestArgumentCaptor.getAllValues().get(0).headers().get("Authorization")); // legit:ignore-secrets
		assertEquals("Bearer newToken", requestArgumentCaptor.getAllValues().get(1).headers().get("Authorization")); // legit:ignore-secrets
		verify(connection).removeExpiredTokens();
	}

	@Test
	void shouldNotRetryNoMoreThanOnceOnUnauthorized() throws SQLException, IOException {
		Call okCall = getMockedCallWithResponse(200, "");
		Call unauthorizedCall = getMockedCallWithResponse(401, "");
		when(okHttpClient.newCall(any())).thenReturn(unauthorizedCall).thenReturn(unauthorizedCall).thenReturn(okCall);
		StatementClient statementClient = new StatementClientImpl(okHttpClient, connection, "ConnA:1.0.9", "ConnB:2.0.9");
		StatementInfoWrapper statementInfoWrapper = StatementUtil.parseToStatementInfoWrappers("show databases").get(0);
		FireboltException ex = assertThrows(FireboltException.class, () -> statementClient.executeSqlStatement(statementInfoWrapper, FIREBOLT_PROPERTIES, false, 5));
		assertEquals(ExceptionType.UNAUTHORIZED, ex.getType());
		verify(okHttpClient, times(2)).newCall(any());
		verify(connection, times(2)).removeExpiredTokens();
	}

	@ParameterizedTest
	@CsvSource({
			"db1,use db1,database=db1,db1",
			"db1,use db2,database=db2,db2",
			"db1,use db2,,db1", // no header returned
			"db1,use db2,database=db3,db3", // use db2 but switched to db3

			"db1,use database db1,database=db1,db1",
			"db1,use database db2,database=db2,db2",
			"db1,use database db2,,db1", // no header returned
			"db1,use database db2,database=db3,db3" // use db2 but switched to db3
	})
	void useDatabase(String oldDb, String command, String responseHeader, String expectedDb) throws SQLException, IOException {
		try (FireboltConnection connection = use("database", oldDb, command, responseHeader)) {
			assertEquals(expectedDb, connection.getSessionProperties().getDatabase());
		}
	}

	@ParameterizedTest
	@CsvSource({
			"e1,use engine e1,engine=e1,e1",
			"e1,use engine e2,engine=e2,e2",
			"e1,use engine e2,,e1", // no header returned
			"e1,use engine e2,engine=e3,e3" // use e2 but switched to e3
	})
	void useEngine(String oldEngine, String command, String responseHeader, String expectedEngine) throws SQLException, IOException {
		try (FireboltConnection connection = use("engine", oldEngine, command, responseHeader)) {
			assertEquals(expectedEngine, connection.getSessionProperties().getEngine());
		}
	}

	@Test
	void useChangeSeveralProperties() throws SQLException, IOException {
		Properties props = new Properties();
		props.setProperty("database", "db1");
		props.setProperty("engine", "e1");
		props.setProperty("account_id", "a1");
		props.setProperty("compress", "false");
		try (FireboltConnection connection = use(1, props, "does not matter", Map.of(
				HEADER_UPDATE_PARAMETER, List.of("database=db2", "engine=e2", "account_id=a1", "addition=something else"),
				HEADER_UPDATE_ENDPOINT, List.of("http://other.com?foo=bar")))) {
			FireboltProperties fbProps = connection.getSessionProperties();
			assertEquals("db2", fbProps.getDatabase());
			assertEquals("e2", fbProps.getEngine());
			assertEquals("a1", fbProps.getAccountId());
			assertEquals("other.com", connection.getEndpoint());
			Map<String, String> additionalProperties = fbProps.getAdditionalProperties();
			assertEquals("something else", additionalProperties.get("addition"));
			assertEquals("bar", additionalProperties.get("foo"));
		}
	}

	@ParameterizedTest(name = "infra version:{0}")
	@CsvSource({
			"1,https://api.app.firebolt.io/?database=db1&two=second&three=third&compress=1&one=first,https://api.app.firebolt.io/?database=db1&output_format=TabSeparatedWithNamesAndTypes&compress=1",
			"2,https://api.app.firebolt.io/?database=db1&account_id=a1&engine=e1&compress=1&one=first&two=second&three=third&query_label=[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12},https://api.app.firebolt.io/?database=db1&account_id=a1&output_format=TabSeparatedWithNamesAndTypes&engine=e1&compress=1&query_label=[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}"
	})
	void useResetSession(int infraVersion, String useUrl, String select1Url) throws SQLException, IOException {
		Properties props = new Properties();
		props.setProperty("database", "db1");
		props.setProperty("engine", "e1");
		props.setProperty("account_id", "a1");
		props.setProperty("one", "first");
		try (FireboltConnection connection = use(infraVersion, props, "does not matter", Map.of(HEADER_UPDATE_PARAMETER, List.of("two=second")))) {
			assertEquals(Map.of("one", "first", "two", "second"), connection.getSessionProperties().getAdditionalProperties());

			// run set statement
			Call setCall = getMockedCallWithResponse(200, "");
			Call select1Call = getMockedCallWithResponse(200, "");
			when(okHttpClient.newCall(any())).thenReturn(setCall, select1Call);
			connection.createStatement().executeUpdate("set three=third");
			assertEquals(Map.of("one", "first", "two", "second", "three", "third"), connection.getSessionProperties().getAdditionalProperties());

			// now reset the session
			Call useCall = getMockedCallWithResponse(200, "", Map.of(HEADER_RESET_SESSION, List.of("")));

			when(okHttpClient.newCall(argThat(new ArgumentMatcher<Request>() {
				private final String[] useUrls = {useUrl, select1Url};
				private int i = 0;
				@Override
				public boolean matches(Request request) {
					return urlsComparator(useUrls[i++], request.url().url().toString());
				}
			}))).thenReturn(useCall, select1Call);
			connection.createStatement().executeUpdate("also does not matter");
			// one->first remains here because this is initial property; it should not be removed during session reset
			assertEquals(Map.of("one", "first"), connection.getSessionProperties().getAdditionalProperties());
		}
	}

	/**
	 * Compares given URL template with actual URL as following: host and then each query parameter.
	 * Each string element of template is interpreted as regular expression, each element of actual
	 * URL as just string.
	 * @param template
	 * @param actual
	 * @return true if matches, false otherwise
	 */
	private boolean urlsComparator(String template, String actual) {
		URL templateUrl = UrlUtil.createUrl(template);
		URL actualUrl = UrlUtil.createUrl(actual);
		if (!actualUrl.getHost().matches(templateUrl.getHost())) {
			return false;
		}
		Map<String, String> templateParameters = UrlUtil.getQueryParameters(templateUrl);
		Map<String, String> actualParameters = UrlUtil.getQueryParameters(actualUrl);
		if (actualParameters.size() != templateParameters.size()) {
			return false;
		}
		for (Entry<String, String> actualParameter : actualParameters.entrySet()) {
			if (!actualParameter.getValue().matches(templateParameters.get(actualParameter.getKey()))) {
				return false;
			}
		}
		return true;
	}

	private FireboltConnection use(String propName, String propValue, String command, String responseHeadersStr) throws SQLException, IOException {
		Map<String, List<String>> responseHeaders = responseHeadersStr == null ? Map.of() : Map.of(HEADER_UPDATE_PARAMETER, List.of(responseHeadersStr.split("\\s*,\\s*")));
		Properties props = new Properties();
		props.setProperty(propName, propValue);
		try (FireboltConnection connection = use(1, props,  command, responseHeaders)) {
			return connection;
		}
	}

	private FireboltConnection use(int mockedInfraVersion, Properties props, String useCommand, Map<String, List<String>> responseHeaders) throws SQLException, IOException {
		props.setProperty(FireboltSessionProperty.CONNECTION_TIMEOUT_MILLIS.getKey(), "0"); // simplifies mocking
		Call useCall = getMockedCallWithResponse(200, "", responseHeaders);
		Call select1Call = getMockedCallWithResponse(200, "");
		when(okHttpClient.newCall(any())).thenReturn(useCall, select1Call);
		FireboltConnection connection = new FireboltConnection("url", props, "0", ParserVersion.CURRENT) {
			{
				this.infraVersion = mockedInfraVersion;
				try {
					connect();
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
			}

			@Override
			protected OkHttpClient getHttpClient(FireboltProperties fireboltProperties) {
				return okHttpClient;
			}

			@Override
			protected FireboltAuthenticationClient createFireboltAuthenticationClient(OkHttpClient httpClient) {
				FireboltAuthenticationClient client = mock(FireboltAuthenticationClient.class);
                try {
                    lenient().when(client.postConnectionTokens(anyString(), any(), any(), any())).thenReturn(new FireboltConnectionTokens("token", 3600));
                } catch (IOException | SQLException e) {
                    throw new IllegalStateException(e);
                }
                return client;
			}

			@Override
			protected void authenticate() {
				sessionProperties = loginProperties;
			}

			@Override
			protected void assertDatabaseExisting(String database) {

			}
		};
		connection.createStatement().executeUpdate(useCommand);
		return connection;
	}

	@ParameterizedTest
	@CsvSource(
			value = {
					"HTTP status code: 401 Unauthorized, body: {\"error\":\"Authentication token is invalid\",\"code\":16,\"message\":\"Authentication token is invalid\",\"details\":[{\"@type\":\"type.googleapis.com/google.rpc.DebugInfo\",\"stack_entries\":[],\"detail\":\"failed to get user_id from fawkes: entity not found\"}]}; Please associate user with your service account.",
					"Engine MyEngine does not exist or not authorized; Please grant at least one role to user associated your service account."
			},
			delimiter = ';')
	void shouldThrowUnauthorizedExceptionWhenNoAssociatedUser(String serverErrorMessage, String exceptionMessage) throws SQLException, IOException {
		when(connection.getAccessToken()).thenReturn(Optional.of("token"));
		Call unauthorizedCall = getMockedCallWithResponse(500, serverErrorMessage);

		when(okHttpClient.newCall(any())).thenReturn(unauthorizedCall);
		StatementClient statementClient = new StatementClientImpl(okHttpClient, connection, "ConnA:1.0.9", "ConnB:2.0.9");
		StatementInfoWrapper statementInfoWrapper = StatementUtil.parseToStatementInfoWrappers("show databases").get(0);
		FireboltException exception = assertThrows(FireboltException.class, () -> statementClient.executeSqlStatement(statementInfoWrapper, FIREBOLT_PROPERTIES, false, 5));
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
		StatementClient statementClient = new StatementClientImpl(okHttpClient, connection, "", "");
		StatementInfoWrapper statementInfoWrapper = StatementUtil.parseToStatementInfoWrappers("select 1").get(0);
		FireboltException ex = assertThrows(FireboltException.class, () -> statementClient.executeSqlStatement(statementInfoWrapper, FIREBOLT_PROPERTIES, false, 5));
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
		lenient().when(response.header(anyString())).then((Answer<String>) invocation -> {
            String name = invocation.getArgument(0);
            List<String> headers = response.headers(name);
            return headers.isEmpty() ? null : headers.get(headers.size() - 1);
        });

		ResponseBody body = mock(ResponseBody.class);
		lenient().when(response.body()).thenReturn(body);
		lenient().when(body.bytes()).thenReturn(content.getBytes());
		lenient().when(body.byteStream()).thenReturn(new ByteArrayInputStream(content.getBytes()));
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