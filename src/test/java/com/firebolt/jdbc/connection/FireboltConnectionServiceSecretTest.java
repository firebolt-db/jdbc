package com.firebolt.jdbc.connection;

import com.firebolt.jdbc.cache.CacheService;
import com.firebolt.jdbc.cache.CacheType;
import com.firebolt.jdbc.cache.ConnectionCache;
import com.firebolt.jdbc.cache.DatabaseOptions;
import com.firebolt.jdbc.cache.EngineOptions;
import com.firebolt.jdbc.cache.key.CacheKey;
import com.firebolt.jdbc.cache.key.ClientSecretCacheKey;
import com.firebolt.jdbc.client.account.FireboltAccountRetriever;
import com.firebolt.jdbc.client.gateway.GatewayUrlResponse;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.service.FireboltEngineVersion2Service;
import com.firebolt.jdbc.service.FireboltGatewayUrlService;
import com.firebolt.jdbc.statement.FireboltStatement;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import com.firebolt.jdbc.statement.StatementInfoWrapper;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;

import static com.firebolt.jdbc.connection.settings.FireboltSessionProperty.HOST;
import static java.lang.String.format;
import static java.sql.Connection.TRANSACTION_NONE;
import static java.sql.Connection.TRANSACTION_READ_COMMITTED;
import static java.sql.Connection.TRANSACTION_READ_UNCOMMITTED;
import static java.sql.Connection.TRANSACTION_REPEATABLE_READ;
import static java.sql.Connection.TRANSACTION_SERIALIZABLE;
import static java.util.stream.Collectors.toMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FireboltConnectionServiceSecretTest extends FireboltConnectionTest {
    private static final String SYSTEM_ENGINE_URL = "jdbc:firebolt:db?env=dev&account=dev";
    private static final String CONNECTION_URL_WITH_ENGINE_AND_DB = "jdbc:firebolt:db?env=dev&account=dev&engine=my_engine";

    private static final String AN_ACCESS_TOKEN = "my access token";
    private static final String ACCOUNT_NAME = "dev";

    private static final String ENGINE_NAME = "my_engine";
    private static final String DB_NAME = "db";

    private static final String SYSTEM_ENGINE_URL_FOR_DEV_ACCOUNT = "https://someurl.com";

    private static final String USER_ENGINE_ENDPOINT = "https://my_engine_endpoint.com";
    private static final String A_CONNECTION_ID = "a_connection_id";
    private static final String ANOTHER_CONNECTION_ID = "another_connection_id";

    @Mock
    private FireboltEngineVersion2Service fireboltEngineVersion2Service;
    @Mock
    private CacheService mockCacheService;

    @Mock
    private FireboltConnectionTokens mockFireboltConnectionTokens;
    @Mock
    private ConnectionIdGenerator mockConnectionIdGenerator;
    @Captor
    private ArgumentCaptor<ConnectionCache> connectionCacheArgumentCaptor;
    @Captor
    private ArgumentCaptor<ClientSecretCacheKey> clientSecretCacheKeyArgumentCaptor;

    private CacheKey cacheKey;

    public FireboltConnectionServiceSecretTest() {
        super("jdbc:firebolt:db?env=dev&engine=eng&account=dev");
    }

    @BeforeEach
    void setupMethod() throws SQLException{
        super.init();

        Engine engine = new Engine(USER_ENGINE_ENDPOINT, "id123", ENGINE_NAME, DB_NAME, null);
        lenient().when(fireboltEngineVersion2Service.getEngine(any(), any(), any(), any())).thenReturn(engine);

        lenient().when(fireboltAuthenticationService.getConnectionTokens(eq("https://api.dev.firebolt.io:443"), any()))
                .thenReturn(mockFireboltConnectionTokens);
        lenient().when(mockFireboltConnectionTokens.getAccessToken()).thenReturn(AN_ACCESS_TOKEN);

        lenient().when(fireboltGatewayUrlService.getUrl(AN_ACCESS_TOKEN, ACCOUNT_NAME)).thenReturn(SYSTEM_ENGINE_URL_FOR_DEV_ACCOUNT);

        cacheKey = new ClientSecretCacheKey("somebody", "pa$$word", "dev");

        when(mockConnectionIdGenerator.generateId()).thenReturn(A_CONNECTION_ID);
    }

    @Test
    void shouldNotGetEngineUrlOrDefaultEngineUrlWhenUsingSystemEngine() throws SQLException {
        connectionProperties.put("database", "my_db");
        when(fireboltGatewayUrlService.getUrl(any(), any())).thenReturn("http://my_endpoint");

        try (FireboltConnection connection = createConnection(SYSTEM_ENGINE_URL, connectionProperties)) {
            verify(fireboltEngineService, times(0)).getEngine(argThat(props -> "my_db".equals(props.getDatabase())));
            assertEquals("my_endpoint", connection.getSessionProperties().getHost());
        }
    }

    @Test
    void noAccount() {
        assertEquals("Cannot connect: account is missing", assertThrows(FireboltException.class, () -> createConnection("jdbc:firebolt:db", connectionProperties)).getMessage());
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource({
            "regular engine,&engine=eng",
            "system engine,''"
    })
    void getMetadata(String testName, String engineParameter) throws SQLException {
        try (FireboltConnection connection = createConnection(format("jdbc:firebolt:db?env=dev&account=dev%s", engineParameter), connectionProperties)) {
            DatabaseMetaData dbmd = connection.getMetaData();
            assertFalse(connection.isReadOnly());
            assertFalse(dbmd.isReadOnly());
            assertSame(dbmd, connection.getMetaData());
            connection.close();
            assertThat(assertThrows(SQLException.class, connection::getMetaData).getMessage(), containsString("closed"));
        }
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource({
            "http://the-endpoint,the-endpoint,",
            "https://the-endpoint,the-endpoint,",
            "the-endpoint,the-endpoint,",
            "http://the-endpoint?foo=1&bar=2,the-endpoint,foo=1;bar=2",
            "https://the-endpoint?foo=1&bar=2,the-endpoint,foo=1;bar=2",
            "the-endpoint?foo=1&bar=2,the-endpoint,foo=1;bar=2",
    })
    void checkSystemEngineEndpoint(String gatewayUrl, String expectedHost, String expectedProps) throws SQLException {
        @SuppressWarnings("unchecked") FireboltAccountRetriever<GatewayUrlResponse> fireboltGatewayUrlClient = mock(FireboltAccountRetriever.class);
        when(fireboltGatewayUrlClient.retrieve(any(), any())).thenReturn(new GatewayUrlResponse(gatewayUrl));
        FireboltGatewayUrlService gatewayUrlService = new FireboltGatewayUrlService(fireboltGatewayUrlClient);
        FireboltConnection connection = new FireboltConnectionServiceSecret(Pair.of(SYSTEM_ENGINE_URL, connectionProperties),
                fireboltAuthenticationService, gatewayUrlService, fireboltStatementService, fireboltEngineVersion2Service,
                mockConnectionIdGenerator, mockCacheService);
        FireboltProperties sessionProperties  = connection.getSessionProperties();
        assertEquals(expectedHost, sessionProperties.getHost());
        assertEquals(expectedProps == null ? Map.of() : Arrays.stream(expectedProps.split(";")).map(kv -> kv.split("=")).collect(toMap(kv -> kv[0], kv -> kv[1])), sessionProperties.getAdditionalProperties());
    }

    @Test
    void resettingTheConnectionWouldNotValidateTheConnection() throws SQLException {
        connectionProperties.put("initialRuntimeParam1", "initialParam1Value");

        try (FireboltConnection connection = createConnection(SYSTEM_ENGINE_URL, connectionProperties)) {
            assertEquals("initialParam1Value", connection.getSessionProperties().getAdditionalProperties().get("initialRuntimeParam1"));

            connection.addProperty("additionalRuntimeParam1", "runtimeParam1");
            assertEquals("runtimeParam1", connection.getSessionProperties().getAdditionalProperties().get("additionalRuntimeParam1") );

            // part of the connection there are some calls to the firebolt statements, so reset the mocks before calling reset
            Mockito.reset(fireboltStatementService);

            connection.reset();

            // verify no calls are made on the connection
            verify(fireboltStatementService, never()).execute(any(), any(FireboltProperties.class), any(FireboltStatement.class));

            // runtime properties should be cleared
            assertNull(connection.getSessionProperties().getAdditionalProperties().get("additionalRuntimeParam1"));

            // initial additional property should still be there
            assertEquals("initialParam1Value", connection.getSessionProperties().getAdditionalProperties().get("initialRuntimeParam1"));
        }
    }

    @Test
    void canAddPropertyThatWillValidateConnection() throws SQLException {
        try (FireboltConnection connection = createConnection(SYSTEM_ENGINE_URL, connectionProperties)) {
            // part of the connection there are some calls to the firebolt statements, so reset the mocks before calling reset
            Mockito.reset(fireboltStatementService);

            // by default, it will validate the connection
            connection.addProperty("newProperty", "new value");

            // verify calls are made on the connection
            verify(fireboltStatementService).execute(any(), any(FireboltProperties.class), any(FireboltStatement.class));

            // initial additional property should be there
            assertEquals("new value", connection.getSessionProperties().getAdditionalProperties().get("newProperty"));
        }
    }

    @Test
    void canAddPropertyThatWillNotValidateConnection() throws SQLException {
        try (FireboltConnection connection = createConnection(SYSTEM_ENGINE_URL, connectionProperties)) {
            // part of the connection there are some calls to the firebolt statements, so reset the mocks before calling reset
            Mockito.reset(fireboltStatementService);

            // do not validate the connection
            connection.addProperty("newProperty", "new value", false);

            // verify no calls are made on the connection
            verify(fireboltStatementService, never()).execute(any(), any(FireboltProperties.class), any(FireboltStatement.class));

            // initial additional property should still be there
            assertEquals("new value", connection.getSessionProperties().getAdditionalProperties().get("newProperty"));
        }
    }

    @Test
    void cannotConnectWhenBothClientIdAndSecretAndAccessTokenArePartOfTheConnectionString() {
        Properties propsWithToken = new Properties();
        propsWithToken.setProperty("client_id", "some clientid");
        propsWithToken.setProperty("client_secret", "do_not_tell_anyone");
        propsWithToken.setProperty(HOST.getKey(), "firebolt_stating_url");
        propsWithToken.setProperty("access_token", "some token");
        FireboltException exception = assertThrows(FireboltException.class, () -> createConnection(url, propsWithToken));
        assertEquals("Ambiguity: Both access token and client ID/secret are supplied", exception.getMessage());
        Mockito.verifyNoMoreInteractions(fireboltAuthenticationService);
    }

    @Test
    void willDefaultToConnectionToBeCachedWhenNoConnectionParamIsPassedInUrl() throws SQLException {
        try (FireboltConnection connection = createConnection(SYSTEM_ENGINE_URL, connectionProperties)) {
            assertTrue(connection.isConnectionCachingEnabled());
        }
    }

    @Test
    void willUseConnectionCacheFromConnectionParametersIfPassedIn() throws SQLException {
        // not present in the url, but have it on the connection properties
        connectionProperties.put("cache_connection", "false");
        try (FireboltConnection connection = createConnection(SYSTEM_ENGINE_URL, connectionProperties)) {
            assertFalse(connection.isConnectionCachingEnabled());
        }
    }

    @ParameterizedTest
    @CsvSource(value = {
            "true, true",
            "false, false" })
    void willUseTheConnectionParamToDetectIfCachingIsEnabledOrNot(String actualValue, String expectedValue) throws SQLException {
        // append to the system engine url the cache parameter
        String urlWithCacheConnection = SYSTEM_ENGINE_URL + "&cache_connection=" + actualValue;
        try (FireboltConnection connection = createConnection(urlWithCacheConnection, connectionProperties)) {
            assertEquals(Boolean.parseBoolean(expectedValue), connection.isConnectionCachingEnabled());
        }
    }

    @Test
    void willSetConnectionCacheAsFalseForAnyValueNotBooleanTrue() throws SQLException {
        String urlWithWrongCacheConnectionValue = SYSTEM_ENGINE_URL + "&cache_connection=not_valid_boolean";
        try (FireboltConnection connection = createConnection(urlWithWrongCacheConnectionValue, connectionProperties)) {
            assertFalse(connection.isConnectionCachingEnabled());
        }
    }

    @Test
    void shouldGetEngineUrlWhenEngineIsProvided() throws SQLException {
        connectionProperties.put("engine", "engine");
        when(fireboltEngineVersion2Service.getEngine(any(), any(), any(), any())).thenReturn(new Engine("http://my_endpoint", null, null, null, null));
        try (FireboltConnection fireboltConnection = createConnection(url, connectionProperties)) {
            verify(fireboltEngineVersion2Service).getEngine(argThat(props -> "engine".equals(props.getEngine()) && "db".equals(props.getDatabase())), any(), eq(mockCacheService), any());
            assertEquals("http://my_endpoint", fireboltConnection.getSessionProperties().getHost());
        }
    }

    @Test
    void canCacheJwtTokenAndSystemEngineWhenConnectionIsCachableAndNoUserEngineOrDatabaseIsSet() throws SQLException {
        enableCacheConnection();

        // no cache is present for the key
        lenient().when(mockCacheService.get(cacheKey)).thenReturn(Optional.empty());

        lenient().doNothing().when(mockCacheService).put(eq(cacheKey), any(ConnectionCache.class));

        // system engine url does not have the database or the engine set
        try (FireboltConnection fireboltConnection = createConnection(SYSTEM_ENGINE_URL, connectionProperties)) {
             // one for creating the cache object, then one for jwt and one for system engine url
             verify(mockCacheService, times(3)).put(clientSecretCacheKeyArgumentCaptor.capture(), connectionCacheArgumentCaptor.capture());

             ClientSecretCacheKey clientSecretCacheKey = clientSecretCacheKeyArgumentCaptor.getValue();
             assertEquals(cacheKey.getValue(), clientSecretCacheKey.getValue());

             ConnectionCache connectionCache = connectionCacheArgumentCaptor.getValue();
             assertEquals(A_CONNECTION_ID, connectionCache.getConnectionId());
             assertEquals(AN_ACCESS_TOKEN, connectionCache.getAccessToken());
             assertEquals(SYSTEM_ENGINE_URL_FOR_DEV_ACCOUNT, connectionCache.getSystemEngineUrl());

             // should not try to get the user engine info
             verify(fireboltEngineVersion2Service, never()).getEngine(any(), any(), any(), any());

             FireboltProperties sessionProperties = fireboltConnection.getSessionProperties();
             assertTrue(sessionProperties.isSystemEngine());
             assertFalse(sessionProperties.isCompress());
             assertEquals("someurl.com", sessionProperties.getHost());
        }

    }

    @Test
    void willGetJwtAndSystemEngineUrlFromCacheWhenConnectionCachingIsEnabledAndTheValuesAreInCache() throws SQLException {
        enableCacheConnection();

        // cache is present
        ConnectionCache connectionCache = new ConnectionCache(A_CONNECTION_ID);
        connectionCache.setAccessToken(AN_ACCESS_TOKEN);
        connectionCache.setSystemEngineUrl(SYSTEM_ENGINE_URL_FOR_DEV_ACCOUNT);
        lenient().when(mockCacheService.get(cacheKey)).thenReturn(Optional.of(connectionCache));

        // system engine url does not have the database or the engine set
        try (FireboltConnection fireboltConnection = createConnection(SYSTEM_ENGINE_URL, connectionProperties)) {
            // no new cache object should be set
            verify(mockCacheService, never()).put(any(CacheKey.class), any(ConnectionCache.class));

            // no calls should be made to get the token or to the system engine
            verify(fireboltAuthenticationService, never()).getConnectionTokens(anyString(), any(FireboltProperties.class));
            verify(fireboltGatewayUrlService, never()).getUrl(any(), any());

            // should not try to get the user engine info
            verify(fireboltEngineVersion2Service, never()).getEngine(any(), any(), any(), any());

            FireboltProperties sessionProperties = fireboltConnection.getSessionProperties();
            assertTrue(sessionProperties.isSystemEngine());
            assertFalse(sessionProperties.isCompress());
            assertEquals("someurl.com", sessionProperties.getHost());
        }
    }

    @Test
    void canCacheJwtTokenSystemEngineDatabaseAndUserEngineWhenConnectionIsCachable() throws SQLException {
        enableCacheConnection();

        // no cache is present for the key
        lenient().when(mockCacheService.get(cacheKey)).thenReturn(Optional.empty());
        lenient().doNothing().when(mockCacheService).put(eq(cacheKey), any(ConnectionCache.class));

        // connection url with engine name
        try (FireboltConnection fireboltConnection = createConnection(CONNECTION_URL_WITH_ENGINE_AND_DB, connectionProperties)) {
            // one for initial cache creation, one for jwt token and one for engine url
            verify(mockCacheService, times(3)).put(clientSecretCacheKeyArgumentCaptor.capture(), connectionCacheArgumentCaptor.capture());

            ClientSecretCacheKey clientSecretCacheKey = clientSecretCacheKeyArgumentCaptor.getValue();
            assertEquals(cacheKey.getValue(), clientSecretCacheKey.getValue());

            ConnectionCache connectionCache = connectionCacheArgumentCaptor.getValue();
            assertEquals(A_CONNECTION_ID, connectionCache.getConnectionId());
            assertEquals(AN_ACCESS_TOKEN, connectionCache.getAccessToken());
            assertEquals(SYSTEM_ENGINE_URL_FOR_DEV_ACCOUNT, connectionCache.getSystemEngineUrl());

            verify(fireboltEngineVersion2Service).getEngine(any(), any(), eq(mockCacheService), eq(cacheKey));

            FireboltProperties sessionProperties = fireboltConnection.getSessionProperties();
            assertFalse(sessionProperties.isSystemEngine());
            assertTrue(sessionProperties.isCompress());
            assertEquals("https://my_engine_endpoint.com", sessionProperties.getHost());
            assertEquals(ENGINE_NAME, sessionProperties.getEngine());
            assertEquals(DB_NAME, sessionProperties.getDatabase());
        }

    }

    @Test
    void willGetJwtSystemEngineUrlUserEngineEndpointAndDatabaseFromCacheWhenConnectionCachingIsEnabledAndTheValuesAreInCache() throws SQLException {
        enableCacheConnection();

        // cache is present
        ConnectionCache connectionCache = new ConnectionCache(A_CONNECTION_ID);
        connectionCache.setAccessToken(AN_ACCESS_TOKEN);
        connectionCache.setSystemEngineUrl(SYSTEM_ENGINE_URL_FOR_DEV_ACCOUNT);
        connectionCache.setDatabaseOptions(DB_NAME, new DatabaseOptions(List.of(Pair.of("database", DB_NAME))));
        EngineOptions engineOptions = new EngineOptions(USER_ENGINE_ENDPOINT, List.of(Pair.of("engine", ENGINE_NAME)));
        connectionCache.setEngineOptions(ENGINE_NAME, engineOptions);

        when(mockCacheService.get(cacheKey)).thenReturn(Optional.of(connectionCache));

        // connection url with engine name
        try (FireboltConnection fireboltConnection = createConnection(CONNECTION_URL_WITH_ENGINE_AND_DB, connectionProperties)) {
            // no new cache object should be set
            verify(mockCacheService, never()).put(any(CacheKey.class), any(ConnectionCache.class));

            // no calls should be made to get the token or to the system engine
            verify(fireboltAuthenticationService, never()).getConnectionTokens(anyString(), any(FireboltProperties.class));
            verify(fireboltGatewayUrlService, never()).getUrl(any(), any());

            // the engine options and db options are set as a side effect of this method so all we can check is make sure it is called
            verify(fireboltEngineVersion2Service).getEngine(any(), any(), eq(mockCacheService), eq(cacheKey));

            FireboltProperties sessionProperties = fireboltConnection.getSessionProperties();
            assertFalse(sessionProperties.isSystemEngine());
            assertTrue(sessionProperties.isCompress());
            assertEquals("https://my_engine_endpoint.com", sessionProperties.getHost());
            assertEquals(ENGINE_NAME, sessionProperties.getEngine());
            assertEquals(DB_NAME, sessionProperties.getDatabase());
        }
    }

    @Test
    void willNotAddAdditionalUserAgentHeaderValuesIfConnectionIsNotCachable() throws SQLException{
        disableCacheConnection();
        try (FireboltConnection fireboltConnection = createConnection(SYSTEM_ENGINE_URL, connectionProperties)) {
            assertTrue(fireboltConnection.getConnectionUserAgentHeader().isEmpty());
        }
    }

    @Test
    void willAddUserAgentHeaderWhenConnectionIsCachedByThisConnection() throws SQLException {
        // no cache is present for the key
        when(mockCacheService.get(cacheKey)).thenReturn(Optional.empty());

        try (FireboltConnectionServiceSecret fireboltConnection = (FireboltConnectionServiceSecret) createConnection(SYSTEM_ENGINE_URL, connectionProperties)) {
            String additionalUserAgentValue = fireboltConnection.getConnectionUserAgentHeader().get();
            assertEquals("connId:" + A_CONNECTION_ID, additionalUserAgentValue);
        }
    }

    @Test
    void willAddUserAgentHeaderWhenConnectionIsCachedByAPreviousConnection() throws SQLException {
        // connection is already cached
        ConnectionCache connectionCache = new ConnectionCache(ANOTHER_CONNECTION_ID);
        connectionCache.setCacheSource(CacheType.MEMORY.name().toLowerCase());

        when(mockCacheService.get(cacheKey)).thenReturn(Optional.of(connectionCache));

        try (FireboltConnectionServiceSecret fireboltConnection = (FireboltConnectionServiceSecret) createConnection(SYSTEM_ENGINE_URL, connectionProperties)) {
            String additionalUserAgentValue = fireboltConnection.getConnectionUserAgentHeader().get();
            String expectedUserAgent = "connId:" + A_CONNECTION_ID + "; cachedConnId:" + ANOTHER_CONNECTION_ID
                    + "-memory";
            assertEquals(expectedUserAgent, additionalUserAgentValue);
        }
    }

    @Override
    @Test
    void shouldGetRepeatableReadTransactionIsolation() throws SQLException {
        connectionProperties.put("database", "db");
        try (Connection fireboltConnection = createConnection(url, connectionProperties)) {
            assertEquals(TRANSACTION_REPEATABLE_READ, fireboltConnection.getTransactionIsolation());
            fireboltConnection.setTransactionIsolation(TRANSACTION_REPEATABLE_READ); // should work
            assertEquals(TRANSACTION_REPEATABLE_READ, fireboltConnection.getTransactionIsolation());
            for (int transactionIsolation : new int [] {TRANSACTION_NONE, TRANSACTION_READ_UNCOMMITTED, TRANSACTION_READ_COMMITTED, TRANSACTION_SERIALIZABLE}) {
                assertThrows(SQLFeatureNotSupportedException.class, () -> fireboltConnection.setTransactionIsolation(transactionIsolation));
            }
            // despite the failed attempts to change transaction isolation to unsupported value it remains TRANSACTION_NONE
            assertEquals(TRANSACTION_REPEATABLE_READ, fireboltConnection.getTransactionIsolation());
        }
    }

    private void enableCacheConnection() {
        connectionProperties.put("cache_connection", "true");
    }

    private void disableCacheConnection() {
        connectionProperties.put("cache_connection", "none");
    }

    @Test
    void shouldRollbackTransactionWhenClosingConnectionWithActiveTransaction() throws SQLException {
        try (FireboltConnection connection = createConnection(url, connectionProperties)) {
            when(fireboltStatementService.execute(any(), any(), any())).thenReturn(Optional.empty());

            // Start a transaction
            connection.setAutoCommit(false);
            connection.ensureTransactionForQueryExecution();

            // Verify we're in a transaction
            assertFalse(connection.getAutoCommit());

            // Close the connection - this should trigger a rollback
            connection.close();

            // Verify rollback was called
            ArgumentCaptor<StatementInfoWrapper> statementCaptor = ArgumentCaptor.forClass(StatementInfoWrapper.class);
            verify(fireboltStatementService, times(2))
                    .execute(statementCaptor.capture(), any(), any());

            List<StatementInfoWrapper> statements = statementCaptor.getAllValues();
            assertEquals("BEGIN TRANSACTION", statements.get(0).getSql());
            assertEquals("ROLLBACK", statements.get(1).getSql());
        }
    }

    @Test
    void shouldNotRollbackWhenClosingConnectionWithoutActiveTransaction() throws SQLException {
        try (FireboltConnection connection = createConnection(url, connectionProperties)) {
            // Don't start a transaction - keep auto-commit enabled
            assertTrue(connection.getAutoCommit());

            // Close the connection - this should NOT trigger a rollback
            connection.close();

            // Verify no rollback was called (only connection setup calls)
            ArgumentCaptor<StatementInfoWrapper> statementCaptor = ArgumentCaptor.forClass(StatementInfoWrapper.class);
            verify(fireboltStatementService, atLeast(0))
                    .execute(statementCaptor.capture(), any(), any());

            // Check that no ROLLBACK statement was executed
            List<StatementInfoWrapper> statements = statementCaptor.getAllValues();
            boolean hasRollback = statements.stream()
                    .anyMatch(stmt -> "ROLLBACK".equals(stmt.getSql()));
            assertFalse(hasRollback, "No ROLLBACK statement should be executed when closing without active transaction");
        }
    }

    protected FireboltConnection createConnection(String url, Properties props) throws SQLException {
        return new FireboltConnectionServiceSecret(Pair.of(url, props), fireboltAuthenticationService, fireboltGatewayUrlService,
                fireboltStatementService, fireboltEngineVersion2Service, mockConnectionIdGenerator, mockCacheService);
    }

    @Test
    void shouldStartTransactionAndCommitWhenSwitchingToAutoCommit() throws SQLException {
        try (FireboltConnection connection = createConnection(url, connectionProperties)) {
            when(fireboltStatementService.execute(any(), any(), any())).thenReturn(Optional.empty());
            
            connection.setAutoCommit(false);
            assertFalse(connection.getAutoCommit());

            connection.createStatement().execute("SELECT 1");
            
            connection.setAutoCommit(true);
            assertTrue(connection.getAutoCommit());

            ArgumentCaptor<StatementInfoWrapper> statementCaptor = ArgumentCaptor.forClass(StatementInfoWrapper.class);

            verify(fireboltStatementService, times(3))
                    .execute(statementCaptor.capture(), any(), any());

            List<StatementInfoWrapper> statement = statementCaptor.getAllValues();
            assertEquals("BEGIN TRANSACTION", statement.get(0).getSql());
            assertEquals("SELECT 1", statement.get(1).getSql());
            assertEquals("COMMIT", statement.get(2).getSql());
        }
    }

    @Test
    void shouldThrowExceptionWhenCommittingOrRollbackWithAutoCommitEnabled() throws SQLException {
        try (FireboltConnection connection = createConnection(url, connectionProperties)) {
            assertTrue(connection.getAutoCommit());
            
            FireboltException commitException = assertThrows(FireboltException.class, connection::commit);
            assertEquals("Cannot commit when auto-commit is enabled", commitException.getMessage());
            
            FireboltException rollbackException = assertThrows(FireboltException.class, connection::rollback);
            assertEquals("Cannot rollback when auto-commit is enabled", rollbackException.getMessage());
        }
    }

    @Test
    void shouldThrowExceptionWhenCommittingOrRollbackWithoutActiveTransaction() throws SQLException {
        try (FireboltConnection connection = createConnection(url, connectionProperties)) {
            connection.setAutoCommit(false);
            
            FireboltException commitException = assertThrows(FireboltException.class, connection::commit);
            assertEquals("No transaction is currently active", commitException.getMessage());
            
            FireboltException rollbackException = assertThrows(FireboltException.class, connection::rollback);
            assertEquals("No transaction is currently active", rollbackException.getMessage());
        }
    }

    @Test
    void shouldCommitRollbackAndBeginTransactionSuccessfully() throws SQLException {
        try (FireboltConnection connection = createConnection(url, connectionProperties)) {
            when(fireboltStatementService.execute(any(), any(), any())).thenReturn(Optional.empty());
            
            connection.setAutoCommit(false);
            
            Statement statement1 = connection.createStatement();
            statement1.execute("SELECT 1");
            connection.commit();
            
            Statement statement2 = connection.createStatement();
            statement2.execute("SELECT 2");
            connection.rollback();

            ArgumentCaptor<StatementInfoWrapper> statementCaptor = ArgumentCaptor.forClass(StatementInfoWrapper.class);

            verify(fireboltStatementService, times(6))
                    .execute(statementCaptor.capture(), any(), any());

            List<StatementInfoWrapper> statement = statementCaptor.getAllValues();
            assertEquals("BEGIN TRANSACTION", statement.get(0).getSql());
            assertEquals("SELECT 1", statement.get(1).getSql());
            assertEquals("COMMIT", statement.get(2).getSql());
            assertEquals("BEGIN TRANSACTION", statement.get(3).getSql());
            assertEquals("SELECT 2", statement.get(4).getSql());
            assertEquals("ROLLBACK", statement.get(5).getSql());
        }
    }

    @Test
    void shouldHandleTransactionErrorsGracefully() throws SQLException {
        try (FireboltConnection connection = createConnection(url, connectionProperties)) {
            connection.setAutoCommit(false);
            
            doThrow(new SQLException("")).when(fireboltStatementService).execute(any(), any(), any());
            
            FireboltException exception = assertThrows(FireboltException.class,
                    connection::ensureTransactionForQueryExecution);
            assertEquals("Could not start transaction for query execution", exception.getMessage());
        }
    }

    @Test
    void shouldHandleCommitErrorsGracefully() throws SQLException {
        try (FireboltConnection connection = createConnection(url, connectionProperties)) {
            connection.setAutoCommit(false);
            
            connection.ensureTransactionForQueryExecution();
            
            doThrow(new SQLException("Commit failed")).when(fireboltStatementService).execute(any(), any(), any());
            
            FireboltException exception = assertThrows(FireboltException.class, connection::commit);
            assertEquals("Could not commit the transaction", exception.getMessage());
        }
    }

    @Test
    void shouldHandleRollbackErrorsGracefully() throws SQLException {
        try (FireboltConnection connection = createConnection(url, connectionProperties)) {
            connection.setAutoCommit(false);
            
            connection.ensureTransactionForQueryExecution();
            
            doThrow(new SQLException("Rollback failed")).when(fireboltStatementService).execute(any(), any(), any());
            
            FireboltException exception = assertThrows(FireboltException.class, connection::rollback);
            assertEquals("Could not rollback the transaction", exception.getMessage());
        }
    }
}
