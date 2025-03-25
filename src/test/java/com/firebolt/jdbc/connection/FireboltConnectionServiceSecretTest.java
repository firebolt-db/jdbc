package com.firebolt.jdbc.connection;

import com.firebolt.jdbc.client.account.FireboltAccountRetriever;
import com.firebolt.jdbc.client.gateway.GatewayUrlResponse;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.service.FireboltGatewayUrlService;
import com.firebolt.jdbc.statement.FireboltStatement;
import com.firebolt.jdbc.type.ParserVersion;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;

import static com.firebolt.jdbc.connection.settings.FireboltSessionProperty.HOST;
import static java.lang.String.format;
import static java.util.stream.Collectors.toMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FireboltConnectionServiceSecretTest extends FireboltConnectionTest {
    private static final String SYSTEM_ENGINE_URL = "jdbc:firebolt:db?env=dev&account=dev";

    public FireboltConnectionServiceSecretTest() {
        super("jdbc:firebolt:db?env=dev&engine=eng&account=dev");
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
    void notExistingDb() throws SQLException {
        connectionProperties.put("database", "my_db");
        when(fireboltGatewayUrlService.getUrl(any(), any())).thenReturn("http://my_endpoint");
        when(fireboltEngineService.doesDatabaseExist("my_db")).thenReturn(false);
        assertEquals("Database my_db does not exist", assertThrows(FireboltException.class, () -> createConnection("jdbc:firebolt:?env=dev&account=dev", connectionProperties)).getMessage());
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
                fireboltAuthenticationService, gatewayUrlService, fireboltStatementService, fireboltEngineService,
                ParserVersion.CURRENT);
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

    protected FireboltConnection createConnection(String url, Properties props) throws SQLException {
        return new FireboltConnectionServiceSecret(url, props, fireboltAuthenticationService, fireboltGatewayUrlService,
                fireboltStatementService, fireboltEngineService, ParserVersion.CURRENT);
    }
}
