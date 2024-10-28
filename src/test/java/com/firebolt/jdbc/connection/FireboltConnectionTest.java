package com.firebolt.jdbc.connection;

import com.firebolt.jdbc.CheckedBiFunction;
import com.firebolt.jdbc.CheckedFunction;
import com.firebolt.jdbc.client.account.FireboltAccount;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.ExceptionType;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.service.FireboltAccountIdService;
import com.firebolt.jdbc.service.FireboltAuthenticationService;
import com.firebolt.jdbc.service.FireboltEngineInformationSchemaService;
import com.firebolt.jdbc.service.FireboltGatewayUrlService;
import com.firebolt.jdbc.service.FireboltStatementService;
import com.firebolt.jdbc.statement.StatementInfoWrapper;
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
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.firebolt.jdbc.connection.settings.FireboltSessionProperty.ACCESS_TOKEN;
import static com.firebolt.jdbc.connection.settings.FireboltSessionProperty.CLIENT_ID;
import static com.firebolt.jdbc.connection.settings.FireboltSessionProperty.CLIENT_SECRET;
import static com.firebolt.jdbc.connection.settings.FireboltSessionProperty.HOST;
import static java.sql.Connection.TRANSACTION_READ_COMMITTED;
import static java.sql.Connection.TRANSACTION_READ_UNCOMMITTED;
import static java.sql.Connection.TRANSACTION_REPEATABLE_READ;
import static java.sql.Connection.TRANSACTION_SERIALIZABLE;
import static java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.CONCUR_UPDATABLE;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE;
import static java.sql.ResultSet.TYPE_SCROLL_SENSITIVE;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
abstract class FireboltConnectionTest {
	private static final String LOCAL_URL = "jdbc:firebolt:local_dev_db?account=dev&ssl=false&max_query_size=10000000&mask_internal_errors=0&host=localhost";
	private static final String SYSTEM_ENGINE_URL = "jdbc:firebolt:db?env=dev&account=dev";
	private final FireboltConnectionTokens fireboltConnectionTokens = new FireboltConnectionTokens(null, 0);
	@Captor
	private ArgumentCaptor<FireboltProperties> propertiesArgumentCaptor;
	@Captor
	private ArgumentCaptor<StatementInfoWrapper> queryInfoWrapperArgumentCaptor;
	@Mock
	protected FireboltAuthenticationService fireboltAuthenticationService;
	@Mock
	protected FireboltGatewayUrlService fireboltGatewayUrlService;

	@Mock
	protected FireboltEngineInformationSchemaService fireboltEngineService;
	@Mock
	protected FireboltStatementService fireboltStatementService;
	@Mock
	protected FireboltAccountIdService fireboltAccountIdService;
	protected Properties connectionProperties = new Properties();
	private static Connection connection;

	private final String URL;

	protected FireboltConnectionTest(String url) {
		this.URL = url;
	}

	private static Stream<Arguments> unsupported() {
		return Stream.of(
				Arguments.of("createSQLXML", (Executable) () -> connection.createSQLXML()),
				Arguments.of("createStruct", (Executable) () -> connection.createStruct("text", new Object[] {"name"})),

				Arguments.of("prepareCall(sql)", (Executable) () -> connection.prepareCall("select 1")),
				Arguments.of("prepareCall(sql, resultSetType, resultSetConcurrency)", (Executable) () -> connection.prepareCall("select 1", 0, 0)),
				Arguments.of("prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability)", (Executable) () -> connection.prepareCall("select 1", 0, 0, 0)),

				Arguments.of("prepareStatement(sql, autoGeneratedKeys)", (Executable) () -> connection.prepareStatement("select 1", Statement.RETURN_GENERATED_KEYS)),
				Arguments.of("prepareStatement(sql, columnIndexes)", (Executable) () -> connection.prepareStatement("select 1", new int[0])),
				Arguments.of("prepareStatement(sql, columnNames)", (Executable) () -> connection.prepareStatement("select 1", new String[0])),

				Arguments.of("setTransactionIsolation", (Executable) () -> connection.setTransactionIsolation(1)),
				Arguments.of("setSavepoint", (Executable) () -> connection.setSavepoint()),
				Arguments.of("setSavepoint(name)", (Executable) () -> connection.setSavepoint("select 1")),
				Arguments.of("releaseSavepoint(savepoint)", (Executable) () -> connection.releaseSavepoint(mock(Savepoint.class))),

				Arguments.of("setTypeMap", (Executable) () -> connection.setTypeMap(Map.of())),

				Arguments.of("rollback(savepoint)", (Executable) () -> connection.rollback(mock(Savepoint.class)))
		);
	}

	private static Stream<Arguments> empty() {
		return Stream.of(
				Arguments.of("getTypeMap", (Callable<?>) () -> connection.getTypeMap(), Map.of()),
				Arguments.of("getWarnings", (Callable<?>) () -> connection.getWarnings(), null)
		);
	}

	private Engine engine;

	@BeforeEach
	void init() throws SQLException {
		connectionProperties = new Properties();
		connectionProperties.put("client_id", "somebody");
		connectionProperties.put("client_secret", "pa$$word");
		connectionProperties.put("compress", "1");
		lenient().when(fireboltAuthenticationService.getConnectionTokens(eq("https://api.dev.firebolt.io:443"), any()))
				.thenReturn(fireboltConnectionTokens);
		lenient().when(fireboltGatewayUrlService.getUrl(any(), any())).thenReturn("http://foo:8080/bar");
		engine = new Engine("endpoint", "id123", "OK", "noname", null);
		lenient().when(fireboltEngineService.getEngine(any())).thenReturn(engine);
		lenient().when(fireboltEngineService.doesDatabaseExist(any())).thenReturn(true);
		lenient().when(fireboltAccountIdService.getValue(any(), any())).thenReturn(new FireboltAccount("id", "earth", 1));
	}

	@Test
	void shouldInitConnection() throws SQLException {
		try (FireboltConnection fireboltConnection = createConnection(URL, connectionProperties)) {
			assertFalse(fireboltConnection.isClosed());
		}
	}

	@Test
	void shouldNotFetchTokenNorEngineHostForLocalFirebolt() throws SQLException {
		try (FireboltConnection fireboltConnection = createConnection(LOCAL_URL, connectionProperties)) {
			verifyNoInteractions(fireboltAuthenticationService);
			verifyNoInteractions(fireboltGatewayUrlService);
			assertFalse(fireboltConnection.isClosed());
		}
	}

	@Test
	void shouldPrepareStatement() throws SQLException {
		shouldPrepareStatement(Connection::prepareStatement);
	}

	@Test
	void shouldPrepareStatementNoGeneratedKeys() throws SQLException {
		shouldPrepareStatement((connection, sql) -> connection.prepareStatement(sql, Statement.NO_GENERATED_KEYS));
	}

	private void shouldPrepareStatement(CheckedBiFunction<Connection, String, PreparedStatement> preparedStatementFactoryMethod) throws SQLException {
		when(fireboltStatementService.execute(any(), any(), any()))
				.thenReturn(Optional.empty());
		try (FireboltConnection fireboltConnection = createConnection(URL, connectionProperties)) {
			PreparedStatement statement = preparedStatementFactoryMethod.apply(fireboltConnection, "INSERT INTO cars(sales, name) VALUES (?, ?)");
			statement.setObject(1, 500);
			statement.setObject(2, "Ford");
			statement.execute();
			assertNotNull(fireboltConnection);
			assertNotNull(statement);
			verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), any(), any());
			assertEquals("INSERT INTO cars(sales, name) VALUES (500, 'Ford')",
					queryInfoWrapperArgumentCaptor.getValue().getSql());
		}
	}

	@Test
	void shouldCloseAllStatementsOnClose() throws SQLException {
		try (FireboltConnection fireboltConnection = createConnection(URL, connectionProperties)) {
			Statement statement = fireboltConnection.createStatement();
			Statement preparedStatement = fireboltConnection.prepareStatement("test");
			fireboltConnection.close();
			assertTrue(statement.isClosed());
			assertTrue(preparedStatement.isClosed());
			assertTrue(fireboltConnection.isClosed());
		}
	}

	@Test
	void createStatement() throws SQLException {
		try (FireboltConnection fireboltConnection = createConnection(URL, connectionProperties)) {
			assertNotNull(fireboltConnection.createStatement());
		}
	}

	@Test
	void createStatementWithParameters() throws SQLException {
		try (FireboltConnection fireboltConnection = createConnection(URL, connectionProperties)) {
			assertNotNull(fireboltConnection.createStatement(TYPE_FORWARD_ONLY, CONCUR_READ_ONLY));
		}
	}

	@Test
	void unsupportedCreateStatementWithParameters() throws SQLException {
		try (FireboltConnection fireboltConnection = createConnection(URL, connectionProperties)) {
			assertThrows(SQLFeatureNotSupportedException.class, () -> fireboltConnection.createStatement(TYPE_SCROLL_INSENSITIVE, CONCUR_READ_ONLY));
			assertThrows(SQLFeatureNotSupportedException.class, () -> fireboltConnection.createStatement(TYPE_SCROLL_SENSITIVE, CONCUR_READ_ONLY));
			assertThrows(SQLFeatureNotSupportedException.class, () -> fireboltConnection.createStatement(TYPE_SCROLL_INSENSITIVE, CONCUR_UPDATABLE));
			assertThrows(SQLFeatureNotSupportedException.class, () -> fireboltConnection.createStatement(TYPE_SCROLL_SENSITIVE, CONCUR_UPDATABLE));
			assertThrows(SQLFeatureNotSupportedException.class, () -> fireboltConnection.createStatement(TYPE_FORWARD_ONLY, CONCUR_UPDATABLE));
		}
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
		FireboltConnection fireboltConnection = createConnection(URL, connectionProperties);
		assertEquals(expected, getter.apply(fireboltConnection));
		fireboltConnection.close();
		assertThrows(FireboltException.class, () -> getter.apply(fireboltConnection)); // cannot invoke this method on closed connection
	}

	@Test
	void prepareCall() throws SQLException {
		notSupported(c -> c.prepareCall("select 1"));
	}

	@Test
	void unsupportedPrepareStatement() throws SQLException {
		notSupported(c -> c.prepareStatement("select 1", ResultSet.TYPE_SCROLL_INSENSITIVE, CONCUR_READ_ONLY));
		notSupported(c -> c.prepareStatement("select 1", Statement.RETURN_GENERATED_KEYS));
		notSupported(c -> c.prepareStatement("select 1", new int[0]));
		notSupported(c -> c.prepareStatement("select 1", new String[0]));
	}

	@Test
	void prepareStatement() throws SQLException {
		try (FireboltConnection fireboltConnection = createConnection(URL, connectionProperties)) {
			assertNotNull(fireboltConnection.prepareStatement("select 1"));
			assertNotNull(fireboltConnection.prepareStatement("select 1", ResultSet.TYPE_FORWARD_ONLY, CONCUR_READ_ONLY));
			assertNotNull(fireboltConnection.prepareStatement("select 1", ResultSet.TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, HOLD_CURSORS_OVER_COMMIT));
		}
	}

	private <T> void notSupported(CheckedFunction<Connection, T> getter) throws SQLException {
		try (FireboltConnection fireboltConnection = createConnection(URL, connectionProperties)) {
			assertThrows(SQLFeatureNotSupportedException.class, () -> getter.apply(fireboltConnection)); // cannot invoke this method on closed connection
		}
	}

	@Test
	void shouldNotSetNewPropertyWhenConnectionIsNotValidWithTheNewProperty() throws SQLException {
		try (FireboltConnection fireboltConnection = createConnection(URL, connectionProperties)) {
			when(fireboltStatementService.execute(any(), any(), any()))
					.thenThrow(new FireboltException(ExceptionType.TOO_MANY_REQUESTS));
			assertThrows(FireboltException.class,
					() -> fireboltConnection.addProperty(Map.entry("custom_1", "1")));

			verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(),
					propertiesArgumentCaptor.capture(), any());
			assertEquals("1", propertiesArgumentCaptor.getValue().getAdditionalProperties().get("custom_1"));
			assertEquals("SELECT 1", queryInfoWrapperArgumentCaptor.getValue().getSql());
			assertNull(fireboltConnection.getSessionProperties().getAdditionalProperties().get("custom_1"));
		}
	}

	@Test
	void shouldSetNewPropertyWhenConnectionIsValidWithTheNewProperty() throws SQLException {
		when(fireboltStatementService.execute(any(), any(), any()))
				.thenReturn(Optional.empty());

		try (FireboltConnection fireboltConnection = createConnection(URL, connectionProperties)) {
			Entry<String, String> newProperties = Map.entry("custom_1", "1");

			fireboltConnection.addProperty(newProperties);

			verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(),
					propertiesArgumentCaptor.capture(), any());
			assertEquals("1", propertiesArgumentCaptor.getValue().getAdditionalProperties().get("custom_1"));
			assertEquals("1", fireboltConnection.getSessionProperties().getAdditionalProperties().get("custom_1"));
			assertEquals(List.of("SELECT 1"), queryInfoWrapperArgumentCaptor.getAllValues().stream().map(StatementInfoWrapper::getSql).collect(toList()));
		}
	}

	@Test
	void shouldValidateConnectionWhenCallingIsValid() throws SQLException {
		when(fireboltStatementService.execute(any(), any(), any()))
				.thenReturn(Optional.empty());
		try (FireboltConnection fireboltConnection = createConnection(URL, connectionProperties)) {
			fireboltConnection.isValid(500);
			verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(),
					propertiesArgumentCaptor.capture(), any());
			assertEquals(List.of("SELECT 1"), queryInfoWrapperArgumentCaptor.getAllValues().stream().map(StatementInfoWrapper::getSql).collect(toList()));
			assertEquals(Map.of("auto_start_stop_control", "ignore"), propertiesArgumentCaptor.getValue().getAdditionalProperties());
		}
	}

	@Test
	void shouldValidateConnectionWhenCallingIsValidSystemEngine() throws SQLException {
		when(fireboltStatementService.execute(any(), any(), any()))
				.thenReturn(Optional.empty());
		Properties propertiesWithSystemEngine = new Properties(connectionProperties);
		try (FireboltConnection fireboltConnection = createConnection(SYSTEM_ENGINE_URL, propertiesWithSystemEngine)) {
			fireboltConnection.isValid(500);
			verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(),
					propertiesArgumentCaptor.capture(), any());
			assertEquals(List.of("SELECT 1"), queryInfoWrapperArgumentCaptor.getAllValues().stream().map(StatementInfoWrapper::getSql).collect(toList()));
			assertEquals(Map.of("auto_start_stop_control", "ignore"), propertiesArgumentCaptor.getValue().getAdditionalProperties());
		}
	}

	@Test
	void shouldIgnore429WhenValidatingConnection() throws SQLException {
		when(fireboltStatementService.execute(any(), any(), any()))
				.thenThrow(new FireboltException(ExceptionType.TOO_MANY_REQUESTS));
		try (FireboltConnection fireboltConnection = createConnection(URL, connectionProperties)) {
			assertTrue(fireboltConnection.isValid(500));
		}
	}

	@Test
	void shouldReturnFalseWhenValidatingConnectionThrowsAnException() throws SQLException {
		when(fireboltStatementService.execute(any(), any(), any()))
				.thenThrow(new FireboltException(ExceptionType.ERROR));
		try (FireboltConnection fireboltConnection = createConnection(URL, connectionProperties)) {
			assertFalse(fireboltConnection.isValid(500));
		}
	}

	@Test
	void shouldThrowExceptionWhenValidatingConnectionWithNegativeTimeout() throws SQLException {
		try (FireboltConnection fireboltConnection = createConnection(URL, connectionProperties)) {
			assertThrows(FireboltException.class, () -> fireboltConnection.isValid(-1));
		}
	}

	@Test
	void shouldReturnFalseWhenValidatingClosedConnection() throws SQLException {
		try (FireboltConnection fireboltConnection = createConnection(URL, connectionProperties)) {
			fireboltConnection.close();
			assertFalse(fireboltConnection.isValid(50));
		}
	}

	@Test
	void shouldExtractConnectorOverrides() throws SQLException {
		when(fireboltStatementService.execute(any(), any(), any()))
				.thenReturn(Optional.empty());
		connectionProperties.put("user_clients", "ConnA:1.0.9,ConnB:2.8.0");
		connectionProperties.put("user_drivers", "DriverA:2.0.9,DriverB:3.8.0");

		try (FireboltConnection fireboltConnection = createConnection(URL, connectionProperties)) {
			PreparedStatement statement = fireboltConnection.prepareStatement("SELECT 1");
			statement.execute();

			verify(fireboltStatementService).execute(any(), propertiesArgumentCaptor.capture(), any());
			assertNull(propertiesArgumentCaptor.getValue().getAdditionalProperties().get("user_clients"));
			assertNull(propertiesArgumentCaptor.getValue().getAdditionalProperties().get("user_drivers"));
			assertNull(fireboltConnection.getSessionProperties().getAdditionalProperties().get("user_clients"));
			assertNull(fireboltConnection.getSessionProperties().getAdditionalProperties().get("user_drivers"));
		}
	}

	@Test
	void shouldGetEngineNameFromHost() throws SQLException {
		connectionProperties.put("engine", "hello");
		try (FireboltConnection fireboltConnection = createConnection(URL, connectionProperties)) {
			assertEquals(engine.getName(), fireboltConnection.getEngine());
		}
	}

	@Test
	void shouldInitNetworkTimeoutWithPropertyByDefault() throws SQLException {
		connectionProperties.put("socket_timeout_millis", "60");
		try (FireboltConnection fireboltConnection = createConnection(URL, connectionProperties)) {
			assertEquals(60, fireboltConnection.getNetworkTimeout());
		}
	}

	@Test
	void shouldInitConnectionTimeoutWithPropertyByDefault() throws SQLException {
		connectionProperties.put("connection_timeout_millis", "50");
		try (FireboltConnection fireboltConnection = createConnection(URL, connectionProperties)) {
			assertEquals(50, fireboltConnection.getConnectionTimeout());
		}
	}

	@Test
	void shouldCloseConnectionWhenAbortingConnection() throws SQLException, InterruptedException {
		try (FireboltConnection fireboltConnection = createConnection(URL, connectionProperties)) {
			ExecutorService executorService = Executors.newFixedThreadPool(10);
			fireboltConnection.abort(executorService);
			assertFalse(executorService.awaitTermination(1, TimeUnit.SECONDS));
			assertTrue(fireboltConnection.isClosed());
		}
	}

	@Test
	void shouldThrowExceptionIfAbortingWithNullExecutor() throws SQLException, InterruptedException {
		try (FireboltConnection fireboltConnection = createConnection(URL, connectionProperties)) {
			assertThrows(FireboltException.class, () -> fireboltConnection.abort(null));
		}
	}

	@Test
	void shouldRemoveExpiredToken() throws SQLException {
		FireboltProperties fireboltProperties = FireboltProperties.builder().host("host").database("db").port(8080).account("dev").build();
		String url = fireboltProperties.getHttpConnectionUrl();
		try (MockedConstruction<FireboltProperties> mockedFireboltPropertiesConstruction = Mockito.mockConstruction(FireboltProperties.class, (fireboltPropertiesMock, context) -> {
			when(fireboltPropertiesMock.getAccount()).thenReturn(fireboltProperties.getAccount());
			when(fireboltPropertiesMock.getDatabase()).thenReturn(fireboltProperties.getDatabase());
			when(fireboltPropertiesMock.getHttpConnectionUrl()).thenReturn(fireboltProperties.getHttpConnectionUrl());
			when(fireboltPropertiesMock.toBuilder()).thenReturn(fireboltProperties.toBuilder());
			lenient().when(fireboltAuthenticationService.getConnectionTokens(eq(url), argThat(argument -> true))).thenReturn(new FireboltConnectionTokens(null, 0));
			lenient().when(fireboltEngineService.getEngine(any())).thenReturn(new Engine("http://hello", null, null, null, null));
		})) {
			try (FireboltConnection fireboltConnection = createConnection(URL, connectionProperties)) {
				fireboltConnection.removeExpiredTokens();
				verify(fireboltAuthenticationService).removeConnectionTokens(eq(url), argThat(argument -> true));
			}
		}
	}

	@Test
	void shouldReturnConnectionTokenWhenAvailable() throws SQLException {
		String accessToken = "hello";
		FireboltProperties fireboltProperties = FireboltProperties.builder().host("host").database("db").port(8080).account("dev").build();
		String url = fireboltProperties.getHttpConnectionUrl();

        try (MockedConstruction<FireboltProperties> mockedFireboltPropertiesConstruction = Mockito.mockConstruction(FireboltProperties.class, (fireboltPropertiesMock, context) -> {
			when(fireboltPropertiesMock.getAccount()).thenReturn(fireboltProperties.getAccount());
			when(fireboltPropertiesMock.getHttpConnectionUrl()).thenReturn(url);
			when(fireboltPropertiesMock.getDatabase()).thenReturn(fireboltProperties.getDatabase());
			when(fireboltPropertiesMock.toBuilder()).thenReturn(fireboltProperties.toBuilder());
        })) {
			FireboltConnectionTokens connectionTokens = new FireboltConnectionTokens(accessToken, 0);
			when(fireboltAuthenticationService.getConnectionTokens(eq(url), any())).thenReturn(connectionTokens);
			lenient().when(fireboltEngineService.getEngine(any())).thenReturn(new Engine("http://engineHost", null, null, null, null));
			try (FireboltConnection fireboltConnection = createConnection(URL, connectionProperties)) {
				verify(fireboltAuthenticationService).getConnectionTokens(eq(url), argThat(argument -> Objects.equals(argument.getHttpConnectionUrl(), url)));
				assertEquals(accessToken, fireboltConnection.getAccessToken().get());
			}
		}
	}

	@Test
	void shouldNotReturnConnectionTokenWithLocalDb() throws SQLException {
		FireboltProperties fireboltProperties = FireboltProperties.builder().host("localhost").build();
		try (MockedConstruction<FireboltProperties> mockedFireboltPropertiesConstruction = Mockito.mockConstruction(FireboltProperties.class, (fireboltPropertiesMock, context) -> {
			when(fireboltPropertiesMock.getHost()).thenReturn(fireboltProperties.getHost());
			when(fireboltPropertiesMock.toBuilder()).thenReturn(fireboltProperties.toBuilder());
		})) {
			try (FireboltConnection fireboltConnection = createConnection(URL, connectionProperties)) {
				assertEquals(Optional.empty(), fireboltConnection.getAccessToken());
				verifyNoInteractions(fireboltAuthenticationService);
			}
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
		try (FireboltConnection fireboltConnection = createConnection(URL, propsWithToken)) {
			assertEquals(expectedAccessToken, fireboltConnection.getAccessToken().orElse(null));
			Mockito.verifyNoMoreInteractions(fireboltAuthenticationService);
		}
	}

	@Test
	void shouldThrowExceptionIfBothAccessTokenAndUserPasswordAreSupplied() {
		Properties propsWithToken = new Properties();
		propsWithToken.setProperty(ACCESS_TOKEN.getKey(), "my-token");
		propsWithToken.setProperty(CLIENT_ID.getKey(), "my-client");
		propsWithToken.setProperty(CLIENT_SECRET.getKey(), "my-secret");
		assertThrows(SQLException.class, () -> createConnection(URL, propsWithToken));
	}

	@Test
	void shouldSetNetworkTimeout() throws SQLException {
		FireboltProperties fireboltProperties = FireboltProperties.builder().host("localhost").socketTimeoutMillis(5).build();
		try (MockedConstruction<FireboltProperties> mockedFireboltPropertiesConstruction = Mockito.mockConstruction(FireboltProperties.class, (fireboltPropertiesMock, context) -> {
			when(fireboltPropertiesMock.getHost()).thenReturn(fireboltProperties.getHost());
			when(fireboltPropertiesMock.getSocketTimeoutMillis()).thenReturn(fireboltProperties.getSocketTimeoutMillis());
			when(fireboltPropertiesMock.toBuilder()).thenReturn(fireboltProperties.toBuilder());
		})) {
			try (FireboltConnection fireboltConnection = createConnection(URL, connectionProperties)) {
				assertEquals(5, fireboltConnection.getNetworkTimeout());
				fireboltConnection.setNetworkTimeout(null, 1);
				assertEquals(1, fireboltConnection.getNetworkTimeout());
			}
		}
	}

	@Test
	void shouldUseConnectionTimeoutFromProperties() throws SQLException {
		FireboltProperties fireboltProperties = FireboltProperties.builder().host("localhost").connectionTimeoutMillis(20).build();
		try (MockedConstruction<FireboltProperties> mockedFireboltPropertiesConstruction = Mockito.mockConstruction(FireboltProperties.class, (fireboltPropertiesMock, context) -> {
			when(fireboltPropertiesMock.getHost()).thenReturn(fireboltProperties.getHost());
			when(fireboltPropertiesMock.getConnectionTimeoutMillis()).thenReturn(fireboltProperties.getConnectionTimeoutMillis());
			when(fireboltPropertiesMock.toBuilder()).thenReturn(fireboltProperties.toBuilder());
		})) {
			try (FireboltConnection fireboltConnection = createConnection(URL, connectionProperties)) {
				assertEquals(fireboltProperties.getConnectionTimeoutMillis(), fireboltConnection.getConnectionTimeout());
			}
		}
	}

	@Test
	void shouldThrowExceptionWhenTryingToUseClosedConnection() throws SQLException {
		try (Connection fireboltConnection = createConnection(URL, connectionProperties)) {
			fireboltConnection.close();
			assertThrows(FireboltException.class, fireboltConnection::getCatalog);
		}
	}

	@Test
	void shouldUnwrapFireboltConnection() throws SQLException {
		try (Connection fireboltConnection = createConnection(URL, connectionProperties)) {
			assertTrue(fireboltConnection.isWrapperFor(FireboltConnection.class));
			assertEquals(fireboltConnection, fireboltConnection.unwrap(FireboltConnection.class));
		}
	}

	@Test
	void shouldThrowExceptionWhenCannotUnwrap() throws SQLException {
		try (Connection fireboltConnection = createConnection(URL, connectionProperties)) {
			assertFalse(fireboltConnection.isWrapperFor(String.class));
			assertThrows(SQLException.class, () -> fireboltConnection.unwrap(String.class));
		}
	}

	@Test
	void shouldGetDatabaseWhenGettingCatalog() throws SQLException {
		connectionProperties.put("database", "db");
		try (Connection fireboltConnection = createConnection(URL, connectionProperties)) {
			assertEquals("noname", fireboltConnection.getCatalog()); // retrieved engine's DB's name is "noname". Firebolt treats DB as catalog
		}
	}

	@Test
	void shouldGetNoneTransactionIsolation() throws SQLException {
		connectionProperties.put("database", "db");
		try (Connection fireboltConnection = createConnection(URL, connectionProperties)) {
			assertEquals(Connection.TRANSACTION_NONE, fireboltConnection.getTransactionIsolation());
			fireboltConnection.setTransactionIsolation(Connection.TRANSACTION_NONE); // should work
			assertEquals(Connection.TRANSACTION_NONE, fireboltConnection.getTransactionIsolation());
			for (int transactionIsolation : new int [] {TRANSACTION_READ_UNCOMMITTED, TRANSACTION_READ_COMMITTED, TRANSACTION_REPEATABLE_READ, TRANSACTION_SERIALIZABLE}) {
				assertThrows(SQLFeatureNotSupportedException.class, () -> fireboltConnection.setTransactionIsolation(transactionIsolation));
			}
			// despite the failed attempts to change transaction isolation to unsupported value it remains TRANSACTION_NONE
			assertEquals(Connection.TRANSACTION_NONE, fireboltConnection.getTransactionIsolation());
		}
	}

	@Test
	void shouldThrowExceptionWhenPreparingStatementWIthInvalidResultSetType() throws SQLException {
		connectionProperties.put("database", "db");
		try (Connection fireboltConnection = createConnection(URL, connectionProperties)) {
			assertThrows(SQLFeatureNotSupportedException.class,
					() -> fireboltConnection.prepareStatement("any", TYPE_SCROLL_INSENSITIVE, 0));
		}
	}

	@Test
	void createArray() throws SQLException {
		try (Connection fireboltConnection = createConnection(URL, connectionProperties)) {
			Object[] data = new Object[] {"red", "green", "blue"};
			Array array = fireboltConnection.createArrayOf("text", data);
			assertEquals(Types.VARCHAR, array.getBaseType());
			assertArrayEquals(data, (Object[])array.getArray());
		}
	}

	@Test
	void createBlob() throws SQLException, IOException {
		try (Connection fireboltConnection = createConnection(URL, connectionProperties)) {
			Blob blob = fireboltConnection.createBlob();
			String str = "hello";
			blob.setBytes(1, str.getBytes());
			assertEquals(str, new String(blob.getBinaryStream().readAllBytes()));
		}
	}

	@Test
	void createClob() throws SQLException, IOException {
		try (Connection fireboltConnection = createConnection(URL, connectionProperties)) {
			Clob clob = fireboltConnection.createClob();
			String str = "hello";
			clob.setString(1, str);
			assertEquals(str, new String(clob.getAsciiStream().readAllBytes()));
		}
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("unsupported")
	void shouldThrowSQLFeatureNotSupportedException(String name, Executable function) throws SQLException {
		connection = createConnection(URL, connectionProperties);
		assertThrows(SQLFeatureNotSupportedException.class, function);
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("empty")
	void shouldReturnEmptyResult(String name, Callable<?> function, Object expected) throws Exception {
		connection = createConnection(URL, connectionProperties);
		assertEquals(expected, function.call());
	}

	@Test
	void shouldGetEngineUrlWhenEngineIsProvided() throws SQLException {
		connectionProperties.put("engine", "engine");
		when(fireboltEngineService.getEngine(any())).thenReturn(new Engine("http://my_endpoint", null, null, null, null));
		try (FireboltConnection fireboltConnection = createConnection(URL, connectionProperties)) {
			verify(fireboltEngineService).getEngine(argThat(props -> "engine".equals(props.getEngine()) && "db".equals(props.getDatabase())));
			assertEquals("http://my_endpoint", fireboltConnection.getSessionProperties().getHost());
		}
	}

	@Test
	void nativeSql() throws SQLException {
		try (FireboltConnection fireboltConnection = createConnection(URL, connectionProperties)) {
			assertEquals("SELECT 1", fireboltConnection.nativeSQL("SELECT 1"));
		}
	}

	@Test
	void unsupportedNativeSql() throws SQLException {
		try (FireboltConnection fireboltConnection = createConnection(URL, connectionProperties)) {
			assertThrows(SQLException.class, () -> fireboltConnection.nativeSQL("SELECT {d '2001-01-01'} FROM TEST"));
		}
	}

	@Test
	void getClientInfo() throws SQLException {
		try (FireboltConnection fireboltConnection = createConnection(URL, connectionProperties)) {
			Properties info = fireboltConnection.getClientInfo();
			// from URL
			assertEquals("dev", info.getProperty("environment"));
			assertEquals("dev", fireboltConnection.getClientInfo("environment")); 	// key
			assertEquals("dev", fireboltConnection.getClientInfo("env"));			// alias

			// from connectionProperties
			assertEquals("somebody", info.getProperty("client_id"));
			assertEquals("somebody", fireboltConnection.getClientInfo("client_id"));	// key
			assertEquals("somebody", fireboltConnection.getClientInfo("user"));			// alias

			// default value
			assertEquals("60", info.getProperty("tcp_keep_idle"));
			assertEquals("60", fireboltConnection.getClientInfo("tcp_keep_idle"));

			// deprecated - should not appear
			assertFalse(info.containsKey("time_to_live_millis"));
			assertNull(fireboltConnection.getClientInfo("time_to_live_millis"));
		}
	}

	protected abstract FireboltConnection createConnection(String url, Properties props) throws SQLException;
}
