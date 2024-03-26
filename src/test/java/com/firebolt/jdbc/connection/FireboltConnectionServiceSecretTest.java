package com.firebolt.jdbc.connection;

import com.firebolt.jdbc.client.account.FireboltAccountRetriever;
import com.firebolt.jdbc.client.gateway.GatewayUrlResponse;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.service.FireboltGatewayUrlService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Properties;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class FireboltConnectionServiceSecretTest extends FireboltConnectionTest {
    private static final String SYSTEM_ENGINE_URL = "jdbc:firebolt:db?env=dev&account=dev";

    public FireboltConnectionServiceSecretTest() {
        super("jdbc:firebolt:db?env=dev&engine=eng&account=dev");
    }

    @Test
    void shouldNotValidateConnectionWhenCallingIsValidWhenUsingSystemEngine() throws SQLException {
        Properties propertiesWithSystemEngine = new Properties(connectionProperties);
        try (FireboltConnection fireboltConnection = createConnection(SYSTEM_ENGINE_URL, propertiesWithSystemEngine)) {
            fireboltConnection.isValid(500);
            verifyNoInteractions(fireboltStatementService);
        }
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
            "regular engine,&engine=eng,false",
            "system engine,'',true" // system engine is readonly
    })
    void getMetadata(String testName, String engineParameter, boolean readOnly) throws SQLException {
        try (FireboltConnection connection = createConnection(format("jdbc:firebolt:db?env=dev&account=dev%s", engineParameter), connectionProperties)) {
            DatabaseMetaData dbmd = connection.getMetaData();
            assertEquals(readOnly, dbmd.isReadOnly());
        }
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource({
            "http://the-endpoint,the-endpoint",
            "https://the-endpoint,the-endpoint",
            "the-endpoint,the-endpoint",
            "http://the-endpoint?foo=1&bar=2,the-endpoint",
            "https://the-endpoint?foo=1&bar=2,the-endpoint",
            "the-endpoint?foo=1&bar=2,the-endpoint",
    })
    void checkSystemEngineEndpoint(String gatewayUrl, String expectedHost) throws SQLException {
        @SuppressWarnings("unchecked") FireboltAccountRetriever<GatewayUrlResponse> fireboltGatewayUrlClient = mock(FireboltAccountRetriever.class);
        when(fireboltGatewayUrlClient.retrieve(any(), any())).thenReturn(new GatewayUrlResponse(gatewayUrl));
        FireboltGatewayUrlService gatewayUrlService = new FireboltGatewayUrlService(fireboltGatewayUrlClient);
        FireboltConnection connection = new FireboltConnectionServiceSecret(SYSTEM_ENGINE_URL, connectionProperties, fireboltAuthenticationService, gatewayUrlService, fireboltStatementService, fireboltEngineService, fireboltAccountIdService);
        assertEquals(expectedHost, connection.getSessionProperties().getHost());
    }

    @Test
    void shouldNotFetchTokenNorEngineHostForLocalFirebolt() throws SQLException {
        super.shouldNotFetchTokenNorEngineHostForLocalFirebolt();
        verifyNoInteractions(fireboltEngineService);
    }

    protected FireboltConnection createConnection(String url, Properties props) throws SQLException {
        return new FireboltConnectionServiceSecret(url, props, fireboltAuthenticationService, fireboltGatewayUrlService, fireboltStatementService, fireboltEngineService, fireboltAccountIdService);
    }
}
