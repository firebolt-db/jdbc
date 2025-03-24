package com.firebolt.jdbc.connection;

import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.service.FireboltAuthenticationService;
import com.firebolt.jdbc.service.FireboltEngineInformationSchemaService;
import com.firebolt.jdbc.service.FireboltGatewayUrlService;
import com.firebolt.jdbc.service.FireboltStatementService;
import com.firebolt.jdbc.type.ParserVersion;
import java.sql.SQLException;
import java.util.Properties;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LocalhostFireboltConnectionServiceTest  {

    private static final String URL = "jdbc:firebolt:db?env=dev&engine=eng&account=dev";
    private static final String LOCAL_URL = "jdbc:firebolt:local_dev_db?account=dev&ssl=false&max_query_size=10000000&mask_internal_errors=0&host=localhost";

    @Mock
    protected FireboltAuthenticationService fireboltAuthenticationService;
    @Mock
    protected FireboltGatewayUrlService fireboltGatewayUrlService;

    @Mock
    protected FireboltEngineInformationSchemaService fireboltEngineService;
    @Mock
    protected FireboltStatementService fireboltStatementService;

    private Properties connectionProperties;

    @BeforeEach
    void setupClass() {
        connectionProperties = new Properties();
        connectionProperties.setProperty("access_token", "the token");
        connectionProperties.setProperty("account", "account");
        connectionProperties.setProperty("user", "myuser@email.com");
        connectionProperties.setProperty("password", "password");
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

    protected FireboltConnection createConnection(String url, Properties props) throws SQLException {
        return new LocalhostFireboltConnectionServiceSecret(url, props, fireboltAuthenticationService, fireboltGatewayUrlService,
                fireboltStatementService, fireboltEngineService, ParserVersion.CURRENT);

    }
}
