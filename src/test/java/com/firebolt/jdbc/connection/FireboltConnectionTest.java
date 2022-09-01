package com.firebolt.jdbc.connection;

import static java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import java.io.ByteArrayInputStream;
import java.sql.*;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.service.FireboltAuthenticationService;
import com.firebolt.jdbc.service.FireboltEngineService;
import com.firebolt.jdbc.service.FireboltStatementService;
import com.firebolt.jdbc.statement.StatementInfoWrapper;

@ExtendWith(MockitoExtension.class)
class FireboltConnectionTest {

	private static final String URL = "jdbc:firebolt://api.dev.firebolt.io/db";
	private static final String LOCAL_URL = "jdbc:firebolt://localhost:8123/local_dev_db?ssl=false&max_query_size=10000000&use_standard_sql=1&mask_internal_errors=0&firebolt_enable_beta_functions=1&firebolt_case_insensitive_identifiers=1&rest_api_pull_timeout_sec=3600&rest_api_pull_interval_millisec=5000&rest_api_retry_times=10";
	private final FireboltConnectionTokens fireboltConnectionTokens = FireboltConnectionTokens.builder().build();
	@Captor
	ArgumentCaptor<FireboltProperties> propertiesArgumentCaptor;
	@Captor
	ArgumentCaptor<StatementInfoWrapper> queryInfoWrapperArgumentCaptor;
	@Mock
	private FireboltAuthenticationService fireboltAuthenticationService;
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
		when(fireboltStatementService.execute(any(), any(), any())).thenReturn(new ByteArrayInputStream("".getBytes()));
		FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltEngineService, fireboltStatementService);
		PreparedStatement statement = fireboltConnection
				.prepareStatement("INSERT INTO cars(sales, name) VALUES (?, ?)");
		statement.setObject(1, 500);
		statement.setObject(2, "Ford");
		statement.execute();
		assertNotNull(fireboltConnection);
		assertNotNull(statement);
		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), any(), any());
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
	void shouldNotSetNewPropertyWhenConnectionIsNotValidWithTheNewProperty() throws SQLException {
		FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltEngineService, fireboltStatementService);

		when(fireboltStatementService.execute(any(), any(), any()))
				.thenThrow(new IllegalArgumentException("The property is invalid"));
		assertThrows(FireboltException.class,
				() -> fireboltConnection.addProperty(new ImmutablePair<>("custom_1", "1")));

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(),
				propertiesArgumentCaptor.capture(), any());
		assertEquals("1", propertiesArgumentCaptor.getValue().getAdditionalProperties().get("custom_1"));
		assertEquals("SELECT 1", queryInfoWrapperArgumentCaptor.getValue().getSql());
		assertNull(fireboltConnection.getSessionProperties().getAdditionalProperties().get("custom_1"));
	}

	@Test
	void shouldSetNewPropertyWhenConnectionIsValidWithTheNewProperty() throws SQLException {
		when(fireboltStatementService.execute(any(), any(), any())).thenReturn(new ByteArrayInputStream("".getBytes()));
		FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltEngineService, fireboltStatementService);

		Pair<String, String> newProperties = new ImmutablePair<>("custom_1", "1");

		fireboltConnection.addProperty(newProperties);

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(),
				propertiesArgumentCaptor.capture(), any());
		assertEquals("1", propertiesArgumentCaptor.getValue().getAdditionalProperties().get("custom_1"));
		assertEquals("1", fireboltConnection.getSessionProperties().getAdditionalProperties().get("custom_1"));
		assertEquals("SELECT 1", queryInfoWrapperArgumentCaptor.getValue().getSql());
	}

	@Test
	void shouldValidateConnectionWhenCallingIsValid() throws SQLException {
		when(fireboltStatementService.execute(any(), any(), any())).thenReturn(new ByteArrayInputStream("".getBytes()));
		FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltEngineService, fireboltStatementService);
		fireboltConnection.isValid(500);

		verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(),
				propertiesArgumentCaptor.capture(), any());
		assertEquals("SELECT 1", queryInfoWrapperArgumentCaptor.getValue().getSql());
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
		when(fireboltStatementService.execute(any(), any(), any())).thenReturn(new ByteArrayInputStream("".getBytes()));
		connectionProperties.put("user_clients", "ConnA:1.0.9,ConnB:2.8.0");
		connectionProperties.put("user_drivers", "DriverA:2.0.9,DriverB:3.8.0");

		FireboltConnection fireboltConnectionImpl = new FireboltConnection(URL, connectionProperties,
				fireboltAuthenticationService, fireboltEngineService, fireboltStatementService);

		PreparedStatement statement = fireboltConnectionImpl.prepareStatement("SELECT 1");
		statement.execute();

		verify(fireboltStatementService).execute(any(), propertiesArgumentCaptor.capture(), any());
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
		FireboltProperties fireboltProperties = FireboltProperties.builder().host("host").path("/db").port(8080)
				.build();
		try (MockedStatic<FireboltProperties> mockedFireboltProperties = Mockito.mockStatic(FireboltProperties.class)) {
			when(FireboltProperties.of(any())).thenReturn(fireboltProperties);
			FireboltConnectionTokens connectionTokens = FireboltConnectionTokens.builder().accessToken("hello").build();
			when(fireboltAuthenticationService.getConnectionTokens("http://host:8080", fireboltProperties))
					.thenReturn(connectionTokens);
			FireboltConnection fireboltConnection = new FireboltConnection(URL, connectionProperties,
					fireboltAuthenticationService, fireboltEngineService, fireboltStatementService);
			verify(fireboltAuthenticationService).getConnectionTokens("http://host:8080", fireboltProperties);
			assertEquals(connectionTokens, fireboltConnection.getConnectionTokens().get());

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
			assertEquals(Optional.empty(), fireboltConnection.getConnectionTokens());
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
}
