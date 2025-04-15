package com.firebolt.jdbc.connection;

import com.firebolt.jdbc.cache.CacheService;
import com.firebolt.jdbc.cache.CacheType;
import com.firebolt.jdbc.cache.ConnectionCache;
import com.firebolt.jdbc.cache.key.CacheKey;
import com.firebolt.jdbc.cache.key.LocalhostCacheKey;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.service.FireboltAuthenticationService;
import com.firebolt.jdbc.service.FireboltEngineVersion2Service;
import com.firebolt.jdbc.service.FireboltGatewayUrlService;
import com.firebolt.jdbc.service.FireboltStatementService;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static com.firebolt.jdbc.connection.settings.FireboltSessionProperty.ACCESS_TOKEN;
import static com.firebolt.jdbc.connection.settings.FireboltSessionProperty.HOST;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LocalhostFireboltConnectionTest {

    private static final String URL = "jdbc:firebolt:db?env=dev&engine=eng&account=dev";
    private static final String URL_WITHOUT_ACCOUNT = "jdbc:firebolt:db?env=dev&engine=eng";
    private static final String LOCAL_URL = "jdbc:firebolt:local_dev_db?account=dev&ssl=false&max_query_size=10000000&mask_internal_errors=0&host=localhost";

    private static final String CONNECTION_ID = "abc";
    private static final String CONNECTION_ID_2 = "def";

    @Mock
    private FireboltAuthenticationService fireboltAuthenticationService;
    @Mock
    private FireboltGatewayUrlService fireboltGatewayUrlService;
    @Mock
    private FireboltEngineVersion2Service fireboltEngineVersion2Service;
    @Mock
    private FireboltStatementService fireboltStatementService;
    @Mock
    private CacheService cacheService;
    @Mock
    private ConnectionIdGenerator mockConnectionIdGenerator;

    private CacheKey cacheKey;
    private Properties connectionProperties;

    @BeforeEach
    void setupClass() {
        connectionProperties = new Properties();
        connectionProperties.setProperty("access_token", "the token");
        connectionProperties.setProperty("account", "account");
        connectionProperties.setProperty("user", "myuser@email.com");
        connectionProperties.setProperty("password", "password");

        cacheKey = new LocalhostCacheKey("the token");
        when(mockConnectionIdGenerator.generateId()).thenReturn(CONNECTION_ID);
    }

    @ParameterizedTest
    @CsvSource(value = {
            "localhost,access-token,access-token"})
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
    void canConnectToLocalHostIfAccountIsMissing() throws SQLException {
        Properties propsWithToken = new Properties();
        propsWithToken.setProperty(ACCESS_TOKEN.getKey(), "the_access_token");
        assertInstanceOf(LocalhostFireboltConnection.class, createConnection(URL_WITHOUT_ACCOUNT, propsWithToken));
    }

    @Test
    void shouldSetNetworkTimeout() throws SQLException {
        FireboltProperties fireboltProperties = FireboltProperties.builder().host("localhost").account("account").accessToken("my token").socketTimeoutMillis(5).build();
        try (MockedConstruction<FireboltProperties> mockedFireboltPropertiesConstruction = Mockito.mockConstruction(FireboltProperties.class, (fireboltPropertiesMock, context) -> {
            when(fireboltPropertiesMock.getHost()).thenReturn(fireboltProperties.getHost());
            when(fireboltPropertiesMock.getAccount()).thenReturn(fireboltProperties.getAccount());
            when(fireboltPropertiesMock.getAccessToken()).thenReturn(fireboltProperties.getAccessToken());
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
        FireboltProperties fireboltProperties = FireboltProperties.builder().host("localhost").account("account").accessToken("my token").connectionTimeoutMillis(20).build();
        try (MockedConstruction<FireboltProperties> mockedFireboltPropertiesConstruction = Mockito.mockConstruction(FireboltProperties.class, (fireboltPropertiesMock, context) -> {
            when(fireboltPropertiesMock.getHost()).thenReturn(fireboltProperties.getHost());
            when(fireboltPropertiesMock.getConnectionTimeoutMillis()).thenReturn(fireboltProperties.getConnectionTimeoutMillis());
            when(fireboltPropertiesMock.getAccount()).thenReturn(fireboltProperties.getAccount());
            when(fireboltPropertiesMock.getAccessToken()).thenReturn(fireboltProperties.getAccessToken());
            when(fireboltPropertiesMock.toBuilder()).thenReturn(fireboltProperties.toBuilder());
        })) {
            try (FireboltConnection fireboltConnection = createConnection(URL, connectionProperties)) {
                assertEquals(fireboltProperties.getConnectionTimeoutMillis(), fireboltConnection.getConnectionTimeout());
            }
        }
    }

    @Test
    void shouldNotCreateConnectionWhenAccessTokenIsMissing() {
        FireboltProperties fireboltProperties = FireboltProperties.builder().host("localhost").account("account").build();
        try (MockedConstruction<FireboltProperties> mockedFireboltPropertiesConstruction = Mockito.mockConstruction(FireboltProperties.class, (fireboltPropertiesMock, context) -> {
            when(fireboltPropertiesMock.getHost()).thenReturn(fireboltProperties.getHost());
            when(fireboltPropertiesMock.getAccount()).thenReturn(fireboltProperties.getAccount());
            when(fireboltPropertiesMock.toBuilder()).thenReturn(fireboltProperties.toBuilder());
        })) {
            SQLException sqlException = assertThrows(SQLException.class, () -> createConnection(URL, connectionProperties));
            assertEquals("Cannot use localhost host connection without an access token", sqlException.getMessage());
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
    void localhostConnectionsAreCachedByDefault() throws SQLException {
        try (FireboltConnection connection = createConnection(LOCAL_URL, connectionProperties)) {
            assertTrue(connection.isConnectionCachingEnabled());
        }
    }

    @ParameterizedTest
    @CsvSource(value = {
            "true,true",
            "false,false" })
    void canCacheConnectionsToLocalhost(String actualValue, boolean expectedValue) throws SQLException {
        // append to the system engine url the cache parameter
        String urlWithCacheConnection = LOCAL_URL + "&cache_connection=" + actualValue;
        try (FireboltConnection connection = createConnection(urlWithCacheConnection, connectionProperties)) {
            assertEquals(expectedValue, connection.isConnectionCachingEnabled());
        }
    }

    @Test
    void willNotAddAdditionalUserAgentHeaderValuesIfConnectionIsNotCachable() throws SQLException{
        disableCacheConnection();
        try (FireboltConnection fireboltConnection = createConnection(LOCAL_URL, connectionProperties)) {
            assertTrue(fireboltConnection.getConnectionUserAgentHeader().isEmpty());
        }
    }

    @Test
    void willAddUserAgentHeaderWhenConnectionIsCachedByThisConnection() throws SQLException {
        // no cache is present for the key
        when(cacheService.get(cacheKey)).thenReturn(Optional.empty());

        try (FireboltConnectionServiceSecret fireboltConnection = (FireboltConnectionServiceSecret) createConnection(LOCAL_URL, connectionProperties)) {
            String additionalUserAgentValue = fireboltConnection.getConnectionUserAgentHeader().get();
            assertEquals("connId:" + CONNECTION_ID, additionalUserAgentValue);
        }
    }

    @Test
    void willAddUserAgentHeaderWhenConnectionIsCachedByAPreviousConnection() throws SQLException {
        // connection is already cached
        ConnectionCache connectionCache = new ConnectionCache(CONNECTION_ID_2);
        connectionCache.setCacheSource(CacheType.MEMORY.name().toLowerCase());

        when(cacheService.get(cacheKey)).thenReturn(Optional.of(connectionCache));

        try (FireboltConnectionServiceSecret fireboltConnection = (FireboltConnectionServiceSecret) createConnection(LOCAL_URL, connectionProperties)) {
            String additionalUserAgentValue = fireboltConnection.getConnectionUserAgentHeader().get();
            String expectedUserAgent = "connId:" + CONNECTION_ID + ";cachedConnId:" + CONNECTION_ID_2 +"-memory";
            assertEquals(expectedUserAgent, additionalUserAgentValue);
        }
    }

    private void disableCacheConnection() {
        connectionProperties.put("cache_connection", "none");
    }


    protected FireboltConnection createConnection(String url, Properties props) throws SQLException {
        return new LocalhostFireboltConnection(Pair.of(url, props), fireboltAuthenticationService, fireboltGatewayUrlService,
                fireboltStatementService, fireboltEngineVersion2Service, mockConnectionIdGenerator, cacheService);

    }
}
