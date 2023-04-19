package com.firebolt.jdbc.connection;

import static java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import java.sql.*;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.firebolt.jdbc.service.FireboltEngineService;
import com.firebolt.jdbc.service.FireboltGatewayUrlService;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.ExceptionType;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.service.FireboltAuthenticationService;
import com.firebolt.jdbc.service.FireboltStatementService;
import com.firebolt.jdbc.statement.StatementInfoWrapper;

@ExtendWith(MockitoExtension.class)
class FireboltConnectionTest {

	private static final String URL = "jdbc:firebolt:db?env=dev&engine=eng";

	private static final String SYSTEM_ENGINE_URL = "jdbc:firebolt:db?env=dev&engine=system";

	private static final String LOCAL_URL = "jdbc:firebolt:local_dev_db?ssl=false&max_query_size=10000000&use_standard_sql=1&mask_internal_errors=0&firebolt_enable_beta_functions=1&firebolt_case_insensitive_identifiers=1&rest_api_pull_timeout_sec=3600&rest_api_pull_interval_millisec=5000&rest_api_retry_times=10&host=localhost";
	private final FireboltConnectionTokens fireboltConnectionTokens = FireboltConnectionTokens.builder().build();
	@Captor
	private ArgumentCaptor<FireboltProperties> propertiesArgumentCaptor;
	@Captor
	private ArgumentCaptor<StatementInfoWrapper> queryInfoWrapperArgumentCaptor;
	@Mock
	private FireboltAuthenticationService fireboltAuthenticationService;
	@Mock
	private FireboltGatewayUrlService fireboltGatewayUrlService;

	@Mock
	private FireboltEngineService fireboltEngineService;
	@Mock
	private FireboltStatementService fireboltStatementService;
	private Properties connectionProperties = new Properties();

	@BeforeEach
	void init() throws FireboltException {
		connectionProperties = new Properties();
		connectionProperties.put("user", "user");
		connectionProperties.put("password", "pa$$word");
		connectionProperties.put("compress", "1");
		lenient().when(fireboltAuthenticationService.getConnectionTokens(eq("https://api.dev.firebolt.io:443"), any()))
				.thenReturn(fireboltConnectionTokens);
		lenient().when(fireboltGatewayUrlService.getUrl(any(), any())).thenReturn("url");
	}

	@Test
	void shouldInitConnection() throws SQLException {
		FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltGatewayUrlService, fireboltStatementService, fireboltEngineService);
		assertFalse(fireboltConnection.isClosed());
	}

	@Test
	void shouldNotFetchTokenNorEngineHostForLocalFirebolt() throws SQLException {
		FireboltConnection fireboltConnection = new FireboltConnection(LOCAL_URL, connectionProperties,
				fireboltAuthenticationService, fireboltGatewayUrlService, fireboltStatementService, fireboltEngineService);
		verifyNoInteractions(fireboltAuthenticationService);
		verifyNoInteractions(fireboltGatewayUrlService);
		assertFalse(fireboltConnection.isClosed());
	}

	@Test
	void shouldPrepareStatement() throws SQLException {
		when(fireboltStatementService.execute(any(), any(), anyInt(), anyInt(), anyBoolean(),anyBoolean(), any()))
				.thenReturn(Optional.empty());
		FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltGatewayUrlService, fireboltStatementService, fireboltEngineService);
		PreparedStatement statement = fireboltConnection
				.prepareStatement("INSERT INTO cars(sales, name) VALUES (?, ?)");
		statement.setObject(1, 500);
		statement.setObject(2, "Ford");
		statement.execute();
		assertNotNull(fireboltConnection);
		assertNotNull(statement);
		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), any(), anyInt(), anyInt(), anyBoolean(),anyBoolean(), any());
		assertEquals("INSERT INTO cars(sales, name) VALUES (500, 'Ford')",
				queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	@Test
	void shouldCloseAllStatementsOnClose() throws SQLException {
		FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltGatewayUrlService, fireboltStatementService, fireboltEngineService);
		Statement statement = fireboltConnection.createStatement();
		Statement preparedStatement = fireboltConnection.prepareStatement("test");
		fireboltConnection.close();
		assertTrue(statement.isClosed());
		assertTrue(preparedStatement.isClosed());
		assertTrue(fireboltConnection.isClosed());
	}

	@Test
	void shouldNotSetNewPropertyWhenConnectionIsNotValidWithTheNewProperty() throws SQLException {
		FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltGatewayUrlService, fireboltStatementService, fireboltEngineService);

		when(fireboltStatementService.execute(any(), any(), anyInt(), anyInt(), anyBoolean(),anyBoolean(), any()))
				.thenThrow(new FireboltException(ExceptionType.TOO_MANY_REQUESTS));
		assertThrows(FireboltException.class,
				() -> fireboltConnection.addProperty(new ImmutablePair<>("custom_1", "1")));

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(),
				propertiesArgumentCaptor.capture(), anyInt(), anyInt(), anyBoolean(), anyBoolean(), any());
		assertEquals("1", propertiesArgumentCaptor.getValue().getAdditionalProperties().get("custom_1"));
		assertEquals("SELECT 1", queryInfoWrapperArgumentCaptor.getValue().getSql());
		assertNull(fireboltConnection.getSessionProperties().getAdditionalProperties().get("custom_1"));
	}

	@Test
	void shouldSetNewPropertyWhenConnectionIsValidWithTheNewProperty() throws SQLException {
		when(fireboltStatementService.execute(any(), any(), anyInt(), anyInt(), anyBoolean(),anyBoolean(), any()))
				.thenReturn(Optional.empty());
		FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltGatewayUrlService, fireboltStatementService, fireboltEngineService);

		Pair<String, String> newProperties = new ImmutablePair<>("custom_1", "1");

		fireboltConnection.addProperty(newProperties);

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(),
				propertiesArgumentCaptor.capture(), anyInt(), anyInt(), anyBoolean(), anyBoolean(), any());
		assertEquals("1", propertiesArgumentCaptor.getValue().getAdditionalProperties().get("custom_1"));
		assertEquals("1", fireboltConnection.getSessionProperties().getAdditionalProperties().get("custom_1"));
		assertEquals("SELECT 1", queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	@Test
	void shouldValidateConnectionWhenCallingIsValid() throws SQLException {
		when(fireboltStatementService.execute(any(), any(), anyInt(), anyInt(), anyBoolean(), anyBoolean(), any()))
				.thenReturn(Optional.empty());
		FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltGatewayUrlService, fireboltStatementService, fireboltEngineService);
		fireboltConnection.isValid(500);

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(),
				propertiesArgumentCaptor.capture(), anyInt(), anyInt(), anyBoolean(), anyBoolean(), any());
		assertEquals("SELECT 1", queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	@Test
	void shouldNotValidateConnectionWhenCallingIsValidWhenUsingSystemEngine() throws SQLException {
		Properties propertiesWithSystemEngine = new Properties(connectionProperties);
		FireboltConnection fireboltConnection = new FireboltConnection(SYSTEM_ENGINE_URL, propertiesWithSystemEngine,
				fireboltAuthenticationService, fireboltGatewayUrlService, fireboltStatementService, fireboltEngineService);
		fireboltConnection.isValid(500);

		verifyNoInteractions(fireboltStatementService);
	}

	@Test
	void shouldIgnore429WhenValidatingConnection() throws SQLException {
		when(fireboltStatementService.execute(any(), any(), anyInt(), anyInt(), anyBoolean(), anyBoolean(), any()))
				.thenThrow(new FireboltException(ExceptionType.TOO_MANY_REQUESTS));
		FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltGatewayUrlService, fireboltStatementService, fireboltEngineService);
		assertTrue(fireboltConnection.isValid(500));
	}

	@Test
	void shouldReturnFalseWhenValidatingConnectionThrowsAnException() throws SQLException {
		when(fireboltStatementService.execute(any(), any(), anyInt(), anyInt(), anyBoolean(), anyBoolean(), any()))
				.thenThrow(new FireboltException(ExceptionType.ERROR));
		FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltGatewayUrlService, fireboltStatementService, fireboltEngineService);
		assertFalse(fireboltConnection.isValid(500));
	}

	@Test
	void shouldThrowExceptionWhenValidatingConnectionWithNegativeTimeout() throws SQLException {
		FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltGatewayUrlService, fireboltStatementService, fireboltEngineService);
		assertThrows(FireboltException.class, () -> fireboltConnection.isValid(-1));
	}

	@Test
	void shouldReturnFalseWhenValidatingClosedConnection() throws SQLException {
		FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltGatewayUrlService, fireboltStatementService, fireboltEngineService);
		fireboltConnection.close();
		assertFalse(fireboltConnection.isValid(50));
	}

	@Test
	void shouldExtractConnectorOverrides() throws SQLException {
		when(fireboltStatementService.execute(any(), any(), anyInt(), anyInt(), anyBoolean(), anyBoolean(), any()))
				.thenReturn(Optional.empty());
		connectionProperties.put("user_clients", "ConnA:1.0.9,ConnB:2.8.0");
		connectionProperties.put("user_drivers", "DriverA:2.0.9,DriverB:3.8.0");

		FireboltConnection fireboltConnectionImpl = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltGatewayUrlService, fireboltStatementService, fireboltEngineService);

		PreparedStatement statement = fireboltConnectionImpl.prepareStatement("SELECT 1");
		statement.execute();

		verify(fireboltStatementService).execute(any(), propertiesArgumentCaptor.capture(), anyInt(), anyInt(),
				anyBoolean(), anyBoolean(), any());
		assertNull(propertiesArgumentCaptor.getValue().getAdditionalProperties().get("user_clients"));
		assertNull(propertiesArgumentCaptor.getValue().getAdditionalProperties().get("user_drivers"));
		assertNull(fireboltConnectionImpl.getSessionProperties().getAdditionalProperties().get("user_clients"));
		assertNull(fireboltConnectionImpl.getSessionProperties().getAdditionalProperties().get("user_drivers"));
	}

	@Test
	@Disabled("System engine is used until engine_url is available")
	void shouldGetEngineNameFromHost() throws SQLException {
		connectionProperties.put("engine", "hello");
		FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltGatewayUrlService, fireboltStatementService, fireboltEngineService);
		assertEquals("hello", fireboltConnection.getEngine());
	}

	@Test
	void shouldInitNetworkTimeoutWithPropertyByDefault() throws SQLException {
		connectionProperties.put("socket_timeout_millis", "60");
		FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltGatewayUrlService, fireboltStatementService, fireboltEngineService);
		assertEquals(60, fireboltConnection.getNetworkTimeout());
	}

	@Test
	void shouldInitConnectionTimeoutWithPropertyByDefault() throws SQLException {
		connectionProperties.put("connection_timeout_millis", "50");
		FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltGatewayUrlService, fireboltStatementService, fireboltEngineService);
		assertEquals(50, fireboltConnection.getConnectionTimeout());
	}

	@Test
	void shouldCloseConnectionWhenAbortingConnection() throws SQLException, InterruptedException {
		FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltGatewayUrlService, fireboltStatementService, fireboltEngineService);
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
			lenient().when(fireboltEngineService.getEngine(any(), any())).thenReturn(Engine.builder().endpoint("https://hello").build());

			FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
					fireboltAuthenticationService, fireboltGatewayUrlService, fireboltStatementService, fireboltEngineService);
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
			lenient().when(fireboltEngineService.getEngine(any(), any())).thenReturn(Engine.builder().endpoint("https://engineHost").build());
			FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
					fireboltAuthenticationService, fireboltGatewayUrlService, fireboltStatementService, fireboltEngineService);
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
					fireboltAuthenticationService, fireboltGatewayUrlService, fireboltStatementService, fireboltEngineService);
			assertEquals(Optional.empty(), fireboltConnection.getAccessToken());
			verifyNoInteractions(fireboltAuthenticationService);
		}
	}

	@Test
	void shouldSetNetworkTimeout() throws SQLException {
		FireboltProperties fireboltProperties = FireboltProperties.builder().host("localhost").path("/db")
				.socketTimeoutMillis(5).port(8080).build();
		try (MockedStatic<FireboltProperties> mockedFireboltProperties = Mockito.mockStatic(FireboltProperties.class)) {
			when(FireboltProperties.of(any())).thenReturn(fireboltProperties);
			FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
					fireboltAuthenticationService, fireboltGatewayUrlService, fireboltStatementService, fireboltEngineService);
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
					fireboltAuthenticationService, fireboltGatewayUrlService, fireboltStatementService, fireboltEngineService);
			assertEquals(20, fireboltConnection.getConnectionTimeout());
		}
	}

	@Test
	void shouldThrowExceptionWhenTryingToUseClosedConnection() throws SQLException {
		FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltGatewayUrlService, fireboltStatementService, fireboltEngineService);
		fireboltConnection.close();
		assertThrows(FireboltException.class, fireboltConnection::getCatalog);
	}

	@Test
	void shouldUnwrapFireboltConnection() throws SQLException {
		Connection connection = new FireboltConnection(URL, connectionProperties, fireboltAuthenticationService,
				fireboltGatewayUrlService, fireboltStatementService, fireboltEngineService);
		assertTrue(connection.isWrapperFor(FireboltConnection.class));
		assertEquals(connection, connection.unwrap(FireboltConnection.class));
	}

	@Test
	void shouldThrowExceptionWhenCannotUnwrap() throws SQLException {
		try (Connection connection = new FireboltConnection(URL, connectionProperties, fireboltAuthenticationService,
				fireboltGatewayUrlService, fireboltStatementService, fireboltEngineService)) {
			assertFalse(connection.isWrapperFor(String.class));
			assertThrows(SQLException.class, () -> connection.unwrap(String.class));
		}
	}

	@Test
	@Disabled("Db is currently not supported")
	void shouldGetDatabaseWhenGettingCatalog() throws SQLException {
		connectionProperties.put("database", "db");
		try (Connection connection = new FireboltConnection(URL, connectionProperties, fireboltAuthenticationService,
				fireboltGatewayUrlService, fireboltStatementService, fireboltEngineService)) {
			assertEquals("db", connection.getCatalog());
		}
	}

	@Test
	void shouldGetNoneTransactionIsolation() throws SQLException {
		connectionProperties.put("database", "db");
		try (Connection connection = new FireboltConnection(URL, connectionProperties, fireboltAuthenticationService,
				fireboltGatewayUrlService, fireboltStatementService, fireboltEngineService)) {
			assertEquals(Connection.TRANSACTION_NONE, connection.getTransactionIsolation());
		}
	}

	@Test
	void shouldThrowExceptionWhenPreparingStatementWIthInvalidResultSetType() throws SQLException {
		connectionProperties.put("database", "db");
		try (Connection connection = new FireboltConnection(URL, connectionProperties, fireboltAuthenticationService,
				fireboltGatewayUrlService, fireboltStatementService, fireboltEngineService)) {
			assertThrows(SQLFeatureNotSupportedException.class,
					() -> connection.prepareStatement("any", TYPE_SCROLL_INSENSITIVE, 0));
		}
	}

	@Test
	@Disabled("Disabled until engine_url is available")
	void shouldGetDefaultEngineWhenEngineIsNotProvided() throws SQLException {
		connectionProperties.put("engine", "");
		when(fireboltEngineService.getEngine(any(), any())).thenReturn(Engine.builder().name("default_engine").endpoint("http://my-endpoint").build());
		try (FireboltConnection  connection = new FireboltConnection(URL, connectionProperties, fireboltAuthenticationService,
				fireboltGatewayUrlService, fireboltStatementService, fireboltEngineService)) {
			verify(fireboltEngineService).getEngine(null, "db");
			assertEquals("default_engine", connection.getSessionProperties().getEngine());
			assertEquals("my-endpoint", connection.getSessionProperties().getHost());
		}
	}

	@Test
	@Disabled("Disabled until engine_url is available")
	void shouldGetEngineUrlWhenEngineIsProvided() throws SQLException {
		connectionProperties.put("engine", "engine");
		when(fireboltEngineService.getEngine(any(), any())).thenReturn(Engine.builder().endpoint("http://my_endpoint").build());
		try (FireboltConnection connection = new FireboltConnection(URL, connectionProperties, fireboltAuthenticationService,
				fireboltGatewayUrlService, fireboltStatementService, fireboltEngineService)) {
			verify(fireboltEngineService).getEngine("engine", null);
			assertEquals("http://my_endpoint", connection.getSessionProperties().getHost());
		}
	}

	@Test
	@Disabled("Db is currently not supported")
	void shouldNotGetEngineUrlOrDefaultEngineUrlWhenUsingSystemEngine() throws SQLException {
		connectionProperties.put("engine", "system");
		when(fireboltGatewayUrlService.getUrl(any(), any())).thenReturn("http://my_endpoint");

		try (FireboltConnection connection = new FireboltConnection(URL, connectionProperties, fireboltAuthenticationService,
				fireboltGatewayUrlService, fireboltStatementService, fireboltEngineService)) {
			verify(fireboltEngineService, times(0)).getEngine(any(), any());
			assertEquals("http://my_endpoint", connection.getSessionProperties().getHost());
		}
	}
}
