package com.firebolt.jdbc.connection;

import com.firebolt.jdbc.cache.CacheService;
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
import com.firebolt.jdbc.type.ParserVersion;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
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
import static java.util.stream.Collectors.toMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
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

    @Mock
    private FireboltEngineVersion2Service fireboltEngineVersion2Service;
    @Mock
    private CacheService mockCacheService;

    @Mock
    private FireboltConnectionTokens mockFireboltConnectionTokens;

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
        lenient().when(fireboltEngineVersion2Service.getEngine(any(), any())).thenReturn(engine);

        lenient().when(fireboltAuthenticationService.getConnectionTokens(eq("https://api.dev.firebolt.io:443"), any()))
                .thenReturn(mockFireboltConnectionTokens);
        lenient().when(mockFireboltConnectionTokens.getAccessToken()).thenReturn(AN_ACCESS_TOKEN);

        lenient().when(fireboltGatewayUrlService.getUrl(AN_ACCESS_TOKEN, ACCOUNT_NAME)).thenReturn(SYSTEM_ENGINE_URL_FOR_DEV_ACCOUNT);

        cacheKey = new ClientSecretCacheKey("somebody", "pa$$word", "dev");
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
        FireboltConnection connection = new FireboltConnectionServiceSecret(SYSTEM_ENGINE_URL, connectionProperties,
                fireboltAuthenticationService, gatewayUrlService, fireboltStatementService, fireboltEngineVersion2Service,
                ParserVersion.CURRENT, mockCacheService);
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
        when(fireboltEngineVersion2Service.getEngine(any(), any())).thenReturn(new Engine("http://my_endpoint", null, null, null, null));
        try (FireboltConnection fireboltConnection = createConnection(url, connectionProperties)) {
            verify(fireboltEngineVersion2Service).getEngine(argThat(props -> "engine".equals(props.getEngine()) && "db".equals(props.getDatabase())), any());
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
             verify(mockCacheService).put(clientSecretCacheKeyArgumentCaptor.capture(), connectionCacheArgumentCaptor.capture());

             ClientSecretCacheKey clientSecretCacheKey = clientSecretCacheKeyArgumentCaptor.getValue();
             assertEquals(cacheKey.getValue(), clientSecretCacheKey.getValue());

             ConnectionCache connectionCache = connectionCacheArgumentCaptor.getValue();
             Assertions.assertNotNull(connectionCache.getConnectionId());
             assertEquals(AN_ACCESS_TOKEN, connectionCache.getAccessToken());
             assertEquals(SYSTEM_ENGINE_URL_FOR_DEV_ACCOUNT, connectionCache.getSystemEngineUrl());

             // should not try to get the user engine info
             verify(fireboltEngineVersion2Service, never()).getEngine(any(), any());

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
            verify(fireboltEngineVersion2Service, never()).getEngine(any(), any());

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
            verify(mockCacheService).put(clientSecretCacheKeyArgumentCaptor.capture(), connectionCacheArgumentCaptor.capture());

            ClientSecretCacheKey clientSecretCacheKey = clientSecretCacheKeyArgumentCaptor.getValue();
            assertEquals(cacheKey.getValue(), clientSecretCacheKey.getValue());

            ConnectionCache connectionCache = connectionCacheArgumentCaptor.getValue();
            assertNotNull(connectionCache.getConnectionId());
            assertEquals(AN_ACCESS_TOKEN, connectionCache.getAccessToken());
            assertEquals(SYSTEM_ENGINE_URL_FOR_DEV_ACCOUNT, connectionCache.getSystemEngineUrl());

            // the engine options and db options are set as a side effect of this method so all we can check is make sure it is called
            verify(fireboltEngineVersion2Service).getEngine(any(), any());

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
            verify(fireboltEngineVersion2Service).getEngine(any(), any());

            FireboltProperties sessionProperties = fireboltConnection.getSessionProperties();
            assertFalse(sessionProperties.isSystemEngine());
            assertTrue(sessionProperties.isCompress());
            assertEquals("https://my_engine_endpoint.com", sessionProperties.getHost());
            assertEquals(ENGINE_NAME, sessionProperties.getEngine());
            assertEquals(DB_NAME, sessionProperties.getDatabase());
        }
    }

    private void enableCacheConnection() {
        connectionProperties.put("cache_connection", "true");
    }

    protected FireboltConnection createConnection(String url, Properties props) throws SQLException {
        return new FireboltConnectionServiceSecret(url, props, fireboltAuthenticationService, fireboltGatewayUrlService,
                fireboltStatementService, fireboltEngineVersion2Service, ParserVersion.CURRENT, mockCacheService);
    }
}
