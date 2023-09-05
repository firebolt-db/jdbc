package com.firebolt.jdbc.connection;

import com.firebolt.jdbc.CheckedFunction;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.ExceptionType;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.service.FireboltAuthenticationService;
import com.firebolt.jdbc.service.FireboltEngineService;
import com.firebolt.jdbc.service.FireboltStatementService;
import com.firebolt.jdbc.statement.StatementInfoWrapper;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Types;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.firebolt.jdbc.connection.settings.FireboltSessionProperty.ACCESS_TOKEN;
import static com.firebolt.jdbc.connection.settings.FireboltSessionProperty.HOST;
import static com.firebolt.jdbc.connection.settings.FireboltSessionProperty.PASSWORD;
import static com.firebolt.jdbc.connection.settings.FireboltSessionProperty.USER;
import static java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.CONCUR_UPDATABLE;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE;
import static java.sql.ResultSet.TYPE_SCROLL_SENSITIVE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FireboltConnectionTest {

	private static final String URL = "jdbc:firebolt://api.dev.firebolt.io/db";
	private static final String LOCAL_URL = "jdbc:firebolt://localhost:8123/local_dev_db?ssl=false&max_query_size=10000000&use_standard_sql=1&mask_internal_errors=0&firebolt_enable_beta_functions=1&firebolt_case_insensitive_identifiers=1&rest_api_pull_timeout_sec=3600&rest_api_pull_interval_millisec=5000&rest_api_retry_times=10";
	private final FireboltConnectionTokens fireboltConnectionTokens = FireboltConnectionTokens.builder().build();
	@Captor
	private ArgumentCaptor<FireboltProperties> propertiesArgumentCaptor;
	@Captor
	private ArgumentCaptor<StatementInfoWrapper> queryInfoWrapperArgumentCaptor;
	@Mock
	private FireboltAuthenticationService fireboltAuthenticationService;
	@Mock
	private FireboltEngineService fireboltEngineService;
	@Mock
	private FireboltStatementService fireboltStatementService;
	private Properties connectionProperties = new Properties();
	private static Connection connection;

	private static Stream<Arguments> unsupported() {
		return Stream.of(
				Arguments.of("createClob", (Executable) () -> connection.createClob()),
				Arguments.of("createNClob", (Executable) () -> connection.createNClob()),
				Arguments.of("createBlob", (Executable) () -> connection.createBlob()),
				Arguments.of("createSQLXML", (Executable) () -> connection.createSQLXML()),
				Arguments.of("createStruct", (Executable) () -> connection.createStruct("text", new Object[] {"name"})),

				Arguments.of("prepareCall(sql)", (Executable) () -> connection.prepareCall("select 1")),
				Arguments.of("prepareCall(sql, resultSetType, resultSetConcurrency)", (Executable) () -> connection.prepareCall("select 1", 0, 0)),
				Arguments.of("prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability)", (Executable) () -> connection.prepareCall("select 1", 0, 0, 0)),

				Arguments.of("prepareStatement(sql, autoGeneratedKeys)", (Executable) () -> connection.prepareStatement("select 1", Statement.RETURN_GENERATED_KEYS)),
				Arguments.of("prepareStatement(sql, columnIndexes)", (Executable) () -> connection.prepareStatement("select 1", new int[0])),
				Arguments.of("prepareStatement(sql, columnNames)", (Executable) () -> connection.prepareStatement("select 1", new String[0])),

				Arguments.of("setSavepoint", (Executable) () -> connection.setSavepoint()),
				Arguments.of("setSavepoint(name)", (Executable) () -> connection.setSavepoint("select 1")),
				Arguments.of("releaseSavepoint(savepoint)", (Executable) () -> connection.releaseSavepoint(mock(Savepoint.class))),

				Arguments.of("setTypeMap", (Executable) () -> connection.setTypeMap(Map.of())),

				Arguments.of("rollback(savepoint)", (Executable) () -> connection.rollback(mock(Savepoint.class))),
				Arguments.of("nativeSQL", (Executable) () -> connection.nativeSQL("select 1"))
		);
	}

	private static Stream<Arguments> empty() {
		return Stream.of(
				Arguments.of("getClientInfo", (Callable<?>) () -> connection.getClientInfo(), new Properties()),
				Arguments.of("getClientInfo(name)", (Callable<?>) () -> connection.getClientInfo("something"), null),
				Arguments.of("getTypeMap", (Callable<?>) () -> connection.getTypeMap(), Map.of()),
				Arguments.of("getWarnings", (Callable<?>) () -> connection.getWarnings(), null)
		);
	}

	@BeforeEach
	void init() throws FireboltException {
		connectionProperties = new Properties();
		connectionProperties.put("user", "user");
		connectionProperties.put("password", "pa$$word");
		connectionProperties.put("compress", "1");
		lenient().when(fireboltAuthenticationService.getConnectionTokens(eq("https://api.dev.firebolt.io:443"), any()))
				.thenReturn(fireboltConnectionTokens);
		lenient().when(fireboltEngineService.getEngine(any(), any(), any())).thenReturn(mock(Engine.class));
	}

	@Test
	void shouldInitConnection() throws SQLException {
		FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltEngineService, fireboltStatementService);
		assertFalse(fireboltConnection.isClosed());
	}

	@Test
	void shouldNotFetchTokenNorEngineHostForLocalFirebolt() throws SQLException {
		FireboltConnection fireboltConnection = new FireboltConnection(LOCAL_URL, connectionProperties,
				fireboltAuthenticationService, fireboltEngineService, fireboltStatementService);
		verifyNoInteractions(fireboltAuthenticationService);
		verifyNoInteractions(fireboltEngineService);
		assertFalse(fireboltConnection.isClosed());
	}

	@Test
	void shouldPrepareStatement() throws SQLException {
		when(fireboltStatementService.execute(any(), any(), anyInt(), anyInt(), anyBoolean(), any()))
				.thenReturn(Optional.empty());
		FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltEngineService, fireboltStatementService);
		PreparedStatement statement = fireboltConnection
				.prepareStatement("INSERT INTO cars(sales, name) VALUES (?, ?)");
		statement.setObject(1, 500);
		statement.setObject(2, "Ford");
		statement.execute();
		assertNotNull(fireboltConnection);
		assertNotNull(statement);
		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), any(), anyInt(), anyInt(),
				anyBoolean(), any());
		assertEquals("INSERT INTO cars(sales, name) VALUES (500, 'Ford')",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	@Test
	void shouldCloseAllStatementsOnClose() throws SQLException {
		FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltEngineService, fireboltStatementService);
		Statement statement = fireboltConnection.createStatement();
		Statement preparedStatement = fireboltConnection.prepareStatement("test");
		fireboltConnection.close();
		assertTrue(statement.isClosed());
		assertTrue(preparedStatement.isClosed());
		assertTrue(fireboltConnection.isClosed());
	}

	@Test
	void createStatement() throws SQLException {
		FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltEngineService, fireboltStatementService);
		assertNotNull(fireboltConnection.createStatement());
	}

	@Test
	void createStatementWithParameters() throws SQLException {
		FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltEngineService, fireboltStatementService);
		assertNotNull(fireboltConnection.createStatement(TYPE_FORWARD_ONLY, CONCUR_READ_ONLY));
	}

	@Test
	void unsupportedCreateStatementWithParameters() throws SQLException {
		FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltEngineService, fireboltStatementService);
		assertThrows(SQLFeatureNotSupportedException.class, () -> fireboltConnection.createStatement(TYPE_SCROLL_INSENSITIVE, CONCUR_READ_ONLY));
		assertThrows(SQLFeatureNotSupportedException.class, () -> fireboltConnection.createStatement(TYPE_SCROLL_SENSITIVE, CONCUR_READ_ONLY));
		assertThrows(SQLFeatureNotSupportedException.class, () -> fireboltConnection.createStatement(TYPE_SCROLL_INSENSITIVE, CONCUR_UPDATABLE));
		assertThrows(SQLFeatureNotSupportedException.class, () -> fireboltConnection.createStatement(TYPE_SCROLL_SENSITIVE, CONCUR_UPDATABLE));
		assertThrows(SQLFeatureNotSupportedException.class, () -> fireboltConnection.createStatement(TYPE_FORWARD_ONLY, CONCUR_UPDATABLE));
	}

	@Test
	void autoCommit() throws SQLException {
		validateFlag(Connection::getAutoCommit, true);
	}

	@Test
	void readOnly() throws SQLException {
		validateFlag(Connection::isReadOnly, false);
	}

	@Test
	void holdability() throws SQLException {
		validateFlag(Connection::getHoldability, CLOSE_CURSORS_AT_COMMIT);
	}

	@Test
	void schema() throws SQLException {
		validateFlag(Connection::getSchema, null);
	}

	private <T> void validateFlag(CheckedFunction<Connection, T> getter, T expected) throws SQLException {
		FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltEngineService, fireboltStatementService);
		assertEquals(expected, getter.apply(fireboltConnection));
		fireboltConnection.close();
		assertThrows(FireboltException.class, () -> getter.apply(fireboltConnection)); // cannot invoke this method on closed connection
	}

	@Test
	void prepareCall() throws FireboltException {
		notSupported(c -> c.prepareCall("select 1"));
	}

	@Test
	void unsupportedPrepareStatement() throws FireboltException {
		notSupported(c -> c.prepareStatement("select 1", ResultSet.TYPE_SCROLL_INSENSITIVE, CONCUR_READ_ONLY));
		notSupported(c -> c.prepareStatement("select 1", Statement.RETURN_GENERATED_KEYS));
		notSupported(c -> c.prepareStatement("select 1", new int[0]));
		notSupported(c -> c.prepareStatement("select 1", new String[0]));
	}

	@Test
	void prepareStatement() throws SQLException {
		FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltEngineService, fireboltStatementService);
		PreparedStatement ps = fireboltConnection.prepareStatement("select 1", ResultSet.TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
		assertNotNull(ps);
	}

	private <T> void notSupported(CheckedFunction<Connection, T> getter) throws FireboltException {
		FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltEngineService, fireboltStatementService);
		assertThrows(SQLFeatureNotSupportedException.class, () -> getter.apply(fireboltConnection)); // cannot invoke this method on closed connection
	}

	@Test
	void shouldNotSetNewPropertyWhenConnectionIsNotValidWithTheNewProperty() throws SQLException {
		FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltEngineService, fireboltStatementService);

		when(fireboltStatementService.execute(any(), any(), anyInt(), anyInt(), anyBoolean(), any()))
				.thenThrow(new FireboltException(ExceptionType.TOO_MANY_REQUESTS));
		assertThrows(FireboltException.class,
				() -> fireboltConnection.addProperty(new ImmutablePair<>("custom_1", "1")));

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(),
				propertiesArgumentCaptor.capture(), anyInt(), anyInt(), anyBoolean(), any());
		assertEquals("1", propertiesArgumentCaptor.getValue().getAdditionalProperties().get("custom_1"));
		assertEquals("SELECT 1", queryInfoWrapperArgumentCaptor.getValue().getSql());
		assertNull(fireboltConnection.getSessionProperties().getAdditionalProperties().get("custom_1"));
	}

	@Test
	void shouldSetNewPropertyWhenConnectionIsValidWithTheNewProperty() throws SQLException {
		when(fireboltStatementService.execute(any(), any(), anyInt(), anyInt(), anyBoolean(), any()))
				.thenReturn(Optional.empty());
		FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltEngineService, fireboltStatementService);

		Pair<String, String> newProperties = new ImmutablePair<>("custom_1", "1");

		fireboltConnection.addProperty(newProperties);

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(),
				propertiesArgumentCaptor.capture(), anyInt(), anyInt(), anyBoolean(), any());
		assertEquals("1", propertiesArgumentCaptor.getValue().getAdditionalProperties().get("custom_1"));
		assertEquals("1", fireboltConnection.getSessionProperties().getAdditionalProperties().get("custom_1"));
		assertEquals("SELECT 1", queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	@Test
	void shouldValidateConnectionWhenCallingIsValid() throws SQLException {
		when(fireboltStatementService.execute(any(), any(), anyInt(), anyInt(), anyBoolean(), any()))
				.thenReturn(Optional.empty());
		FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltEngineService, fireboltStatementService);
		fireboltConnection.isValid(500);

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(),
				propertiesArgumentCaptor.capture(), anyInt(), anyInt(), anyBoolean(), any());
		assertEquals("SELECT 1", queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	@Test
	void shouldNotValidateConnectionWhenCallingIsValidWhenUsingSystemEngine() throws SQLException {
		Properties propertiesWithSystemEngine = new Properties(connectionProperties);
		propertiesWithSystemEngine.put("engine_name", "system");
		FireboltConnection fireboltConnection = new FireboltConnection(URL, propertiesWithSystemEngine,
				fireboltAuthenticationService, fireboltEngineService, fireboltStatementService);
		fireboltConnection.isValid(500);

		verifyNoInteractions(fireboltStatementService);
	}

	@Test
	void shouldIgnore429WhenValidatingConnection() throws SQLException {
		when(fireboltStatementService.execute(any(), any(), anyInt(), anyInt(), anyBoolean(), any()))
				.thenThrow(new FireboltException(ExceptionType.TOO_MANY_REQUESTS));
		FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltEngineService, fireboltStatementService);
		assertTrue(fireboltConnection.isValid(500));
	}

	@Test
	void shouldReturnFalseWhenValidatingConnectionThrowsAnException() throws SQLException {
		when(fireboltStatementService.execute(any(), any(), anyInt(), anyInt(), anyBoolean(), any()))
				.thenThrow(new FireboltException(ExceptionType.ERROR));
		FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltEngineService, fireboltStatementService);
		assertFalse(fireboltConnection.isValid(500));
	}

	@Test
	void shouldThrowExceptionWhenValidatingConnectionWithNegativeTimeout() throws SQLException {
		FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltEngineService, fireboltStatementService);
		assertThrows(FireboltException.class, () -> fireboltConnection.isValid(-1));
	}

	@Test
	void shouldReturnFalseWhenValidatingClosedConnection() throws SQLException {
		FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltEngineService, fireboltStatementService);
		fireboltConnection.close();
		assertFalse(fireboltConnection.isValid(50));
	}

	@Test
	void shouldExtractConnectorOverrides() throws SQLException {
		when(fireboltStatementService.execute(any(), any(), anyInt(), anyInt(), anyBoolean(), any()))
				.thenReturn(Optional.empty());
		connectionProperties.put("user_clients", "ConnA:1.0.9,ConnB:2.8.0");
		connectionProperties.put("user_drivers", "DriverA:2.0.9,DriverB:3.8.0");

		FireboltConnection fireboltConnectionImpl = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltEngineService, fireboltStatementService);

		PreparedStatement statement = fireboltConnectionImpl.prepareStatement("SELECT 1");
		statement.execute();

		verify(fireboltStatementService).execute(any(), propertiesArgumentCaptor.capture(), anyInt(), anyInt(),
				anyBoolean(), any());
		assertNull(propertiesArgumentCaptor.getValue().getAdditionalProperties().get("user_clients"));
		assertNull(propertiesArgumentCaptor.getValue().getAdditionalProperties().get("user_drivers"));
		assertNull(fireboltConnectionImpl.getSessionProperties().getAdditionalProperties().get("user_clients"));
		assertNull(fireboltConnectionImpl.getSessionProperties().getAdditionalProperties().get("user_drivers"));
	}

	@Test
	void shouldGetEngineNameFromHost() throws SQLException {
		when(fireboltEngineService.getEngineNameFromHost(any())).thenReturn("myHost_345");
		FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltEngineService, fireboltStatementService);
		assertEquals("myHost_345", fireboltConnection.getEngine());
	}

	@Test
	void shouldInitNetworkTimeoutWithPropertyByDefault() throws SQLException {
		connectionProperties.put("socket_timeout_millis", "60");
		FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltEngineService, fireboltStatementService);
		assertEquals(60, fireboltConnection.getNetworkTimeout());
	}

	@Test
	void shouldInitConnectionTimeoutWithPropertyByDefault() throws SQLException {
		connectionProperties.put("connection_timeout_millis", "50");
		FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltEngineService, fireboltStatementService);
		assertEquals(50, fireboltConnection.getConnectionTimeout());
	}

	@Test
	void shouldCloseConnectionWhenAbortingConnection() throws SQLException, InterruptedException {
		FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltEngineService, fireboltStatementService);
		ExecutorService executorService = Executors.newFixedThreadPool(10);
		fireboltConnection.abort(executorService);
		executorService.awaitTermination(1, TimeUnit.SECONDS);
		assertTrue(fireboltConnection.isClosed());
	}

	@Test
	void shouldRemoveExpiredToken() throws SQLException {
		FireboltProperties fireboltProperties = FireboltProperties.builder().host("host").path("/db").port(8080)
				.build();
		try (MockedStatic<FireboltProperties> mockedFireboltProperties = Mockito.mockStatic(FireboltProperties.class)) {
			when(FireboltProperties.of(any())).thenReturn(fireboltProperties);
			when(fireboltAuthenticationService.getConnectionTokens("http://host:8080", fireboltProperties))
					.thenReturn(FireboltConnectionTokens.builder().build());
			FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
					fireboltAuthenticationService, fireboltEngineService, fireboltStatementService);
			fireboltConnection.removeExpiredTokens();
			verify(fireboltAuthenticationService).removeConnectionTokens("http://host:8080", fireboltProperties);
		}
	}

	@Test
	void shouldReturnConnectionTokenWhenAvailable() throws SQLException {
		String accessToken = "hello";
		FireboltProperties fireboltProperties = FireboltProperties.builder().host("host").path("/db").port(8080)
				.build();
		try (MockedStatic<FireboltProperties> mockedFireboltProperties = Mockito.mockStatic(FireboltProperties.class)) {
			when(FireboltProperties.of(any())).thenReturn(fireboltProperties);
			FireboltConnectionTokens connectionTokens = FireboltConnectionTokens.builder().accessToken(accessToken).build();
			when(fireboltAuthenticationService.getConnectionTokens(eq("http://host:8080"), any()))
					.thenReturn(connectionTokens);
			FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
					fireboltAuthenticationService, fireboltEngineService, fireboltStatementService);
			verify(fireboltAuthenticationService).getConnectionTokens("http://host:8080", fireboltProperties);
			assertEquals(accessToken, fireboltConnection.getAccessToken().get());
		}
	}

	@Test
	void shouldNotReturnConnectionTokenWithLocalDb() throws SQLException {
		FireboltProperties fireboltProperties = FireboltProperties.builder().host("localhost").path("/db").port(8080)
				.build();
		try (MockedStatic<FireboltProperties> mockedFireboltProperties = Mockito.mockStatic(FireboltProperties.class)) {
			when(FireboltProperties.of(any())).thenReturn(fireboltProperties);
			FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
					fireboltAuthenticationService, fireboltEngineService, fireboltStatementService);
			assertEquals(Optional.empty(), fireboltConnection.getAccessToken());
			verifyNoInteractions(fireboltAuthenticationService);
		}
	}

	@ParameterizedTest
	@CsvSource(value = {
			"localhost,access-token,access-token",
			"localhost,,", // access token cannot be retrieved from service for localhost
			"my-host,access-token,access-token"})
	void shouldGetConnectionTokenFromProperties(String host, String configuredAccessToken, String expectedAccessToken) throws SQLException {
		Properties propsWithToken = new Properties();
		if (host != null) {
			propsWithToken.setProperty(HOST.getKey(), host);
		}
		if (configuredAccessToken != null) {
			propsWithToken.setProperty(ACCESS_TOKEN.getKey(), configuredAccessToken);
		}
		try (FireboltConnection connection = new FireboltConnection(URL, propsWithToken, fireboltAuthenticationService,
				fireboltEngineService, fireboltStatementService)) {
			assertEquals(expectedAccessToken, connection.getAccessToken().orElse(null));
			Mockito.verifyNoMoreInteractions(fireboltAuthenticationService);
		}
	}

	@Test
	void shouldThrowExceptionIfBothAccessTokenAndUserPasswordAreSupplied() {
		Properties propsWithToken = new Properties();
		propsWithToken.setProperty(ACCESS_TOKEN.getKey(), "my-token");
		propsWithToken.setProperty(USER.getKey(), "my-user");
		propsWithToken.setProperty(PASSWORD.getKey(), "my-password");
		assertThrows(SQLException.class, () -> new FireboltConnection(URL, propsWithToken, fireboltAuthenticationService,
				fireboltEngineService, fireboltStatementService));
	}

	@Test
	void shouldSetNetworkTimeout() throws SQLException {
		FireboltProperties fireboltProperties = FireboltProperties.builder().host("localhost").path("/db")
				.socketTimeoutMillis(5).port(8080).build();
		try (MockedStatic<FireboltProperties> mockedFireboltProperties = Mockito.mockStatic(FireboltProperties.class)) {
			when(FireboltProperties.of(any())).thenReturn(fireboltProperties);
			FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
					fireboltAuthenticationService, fireboltEngineService, fireboltStatementService);
			assertEquals(5, fireboltConnection.getNetworkTimeout());
			fireboltConnection.setNetworkTimeout(null, 1);
			assertEquals(1, fireboltConnection.getNetworkTimeout());
		}
	}

	@Test
	void shouldUseConnectionTimeoutFromProperties() throws SQLException {
		FireboltProperties fireboltProperties = FireboltProperties.builder().host("localhost").path("/db")
				.connectionTimeoutMillis(20).port(8080).build();
		try (MockedStatic<FireboltProperties> mockedFireboltProperties = Mockito.mockStatic(FireboltProperties.class)) {
			when(FireboltProperties.of(any())).thenReturn(fireboltProperties);
			FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
					fireboltAuthenticationService, fireboltEngineService, fireboltStatementService);
			assertEquals(20, fireboltConnection.getConnectionTimeout());
		}
	}

	@Test
	void shouldThrowExceptionWhenTryingToUseClosedConnection() throws SQLException {
		FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltEngineService, fireboltStatementService);
		fireboltConnection.close();
		assertThrows(FireboltException.class, fireboltConnection::getCatalog);
	}

	@Test
	void shouldUnwrapFireboltConnection() throws SQLException {
		Connection connection = new FireboltConnection(URL, connectionProperties, fireboltAuthenticationService,
				fireboltEngineService, fireboltStatementService);
		assertTrue(connection.isWrapperFor(FireboltConnection.class));
		assertEquals(connection, connection.unwrap(FireboltConnection.class));
	}

	@Test
	void shouldThrowExceptionWhenCannotUnwrap() throws SQLException {
		try (Connection connection = new FireboltConnection(URL, connectionProperties, fireboltAuthenticationService,
				fireboltEngineService, fireboltStatementService)) {
			assertFalse(connection.isWrapperFor(String.class));
			assertThrows(SQLException.class, () -> connection.unwrap(String.class));
		}
	}

	@Test
	void shouldGetDatabaseWhenGettingCatalog() throws SQLException {
		connectionProperties.put("database", "db");
		try (Connection connection = new FireboltConnection(URL, connectionProperties, fireboltAuthenticationService,
				fireboltEngineService, fireboltStatementService)) {
			assertEquals("db", connection.getCatalog());
		}
	}

	@Test
	void shouldGetNoneTransactionIsolation() throws SQLException {
		connectionProperties.put("database", "db");
		try (Connection connection = new FireboltConnection(URL, connectionProperties, fireboltAuthenticationService,
				fireboltEngineService, fireboltStatementService)) {
			assertEquals(Connection.TRANSACTION_NONE, connection.getTransactionIsolation());
		}
	}

	@Test
	void shouldThrowExceptionWhenPreparingStatementWIthInvalidResultSetType() throws SQLException {
		connectionProperties.put("database", "db");
		try (Connection connection = new FireboltConnection(URL, connectionProperties, fireboltAuthenticationService,
				fireboltEngineService, fireboltStatementService)) {
			assertThrows(SQLFeatureNotSupportedException.class,
					() -> connection.prepareStatement("any", TYPE_SCROLL_INSENSITIVE, 0));
		}
	}

	@Test
	void createArray() throws SQLException {
		try (Connection connection = new FireboltConnection(URL, connectionProperties, fireboltAuthenticationService,
				fireboltEngineService, fireboltStatementService)) {
			Object[] data = new Object[] {"red", "green", "blue"};
			Array array = connection.createArrayOf("text", data);
			assertEquals(Types.VARCHAR, array.getBaseType());
			assertArrayEquals(data, (Object[])array.getArray());
		}
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("unsupported")
	void shouldThrowSQLFeatureNotSupportedException(String name, Executable function) throws FireboltException {
		connection = new FireboltConnection(URL, connectionProperties, fireboltAuthenticationService,
				fireboltEngineService, fireboltStatementService);
		assertThrows(SQLFeatureNotSupportedException.class, function);
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("empty")
	void shouldReturnEmptyResult(String name, Callable<?> function, Object expected) throws Exception {
		connection = new FireboltConnection(URL, connectionProperties, fireboltAuthenticationService,
				fireboltEngineService, fireboltStatementService);
		assertEquals(expected, function.call());
	}
}
