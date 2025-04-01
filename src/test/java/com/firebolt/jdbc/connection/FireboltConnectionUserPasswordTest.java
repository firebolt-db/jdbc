package com.firebolt.jdbc.connection;

import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.type.ParserVersion;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static com.firebolt.jdbc.connection.FireboltConnectionUserPassword.SYSTEM_ENGINE_NAME;
import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class FireboltConnectionUserPasswordTest extends FireboltConnectionTest {
    private static final String SYSTEM_ENGINE_URL = "jdbc:firebolt:db?env=dev&account=dev&engine=system";

    public FireboltConnectionUserPasswordTest() {
        super("jdbc:firebolt://api.dev.firebolt.io/db");
    }

    @ParameterizedTest
    @CsvSource({
            "'','validPassword','','Cannot connect: username is missing'",
            "'validUser@email.com','','','Cannot connect: password is missing'",
            "'validUser@email.com','validPassword','some access token','Ambiguity: Both access token and username/password are provided'"
    })
    void shouldNotConnectWhenRequiredParametersAreMissing(String username, String password, String accessToken, String expectedErrorMessage) {
        connectionProperties.put("client_id", username);
        connectionProperties.put("client_secret", password);
        connectionProperties.put("access_token", accessToken);
        FireboltException exception = assertThrows(FireboltException.class, () -> createConnection(SYSTEM_ENGINE_URL, connectionProperties));
        assertEquals(expectedErrorMessage, exception.getMessage());
    }

    @Test
    void shouldNotGetEngineUrlOrDefaultEngineUrlWhenUsingSystemEngine() throws SQLException {
        connectionProperties.put("database", "my_db");
        try (FireboltConnection connection = createConnection(SYSTEM_ENGINE_URL, connectionProperties)) {
            verify(fireboltEngineService, times(1)).getEngine(argThat(props -> "my_db".equals(props.getDatabase()) && SYSTEM_ENGINE_NAME.equals(props.getEngine())));
            assertEquals("endpoint", connection.getSessionProperties().getHost());
        }
    }

    @Test
    void noEngineAndDb() throws SQLException {
        try (FireboltConnection connection = createConnection("jdbc:firebolt://api.dev.firebolt.io", connectionProperties)) {
            assertEquals("endpoint", connection.getSessionProperties().getHost());
            assertNotNull(connection.getSessionProperties().getEngine());
            assertFalse(connection.getSessionProperties().isSystemEngine());
        }
    }

    @ParameterizedTest
    @CsvSource({
            "eng",
            "system"
    })
    void getMetadata(String engine) throws SQLException {
        try (FireboltConnection connection = createConnection(format("jdbc:firebolt:db?env=dev&engine=%s&account=dev", engine), connectionProperties)) {
            DatabaseMetaData dbmd = connection.getMetaData();
            assertFalse(connection.isReadOnly());
            assertFalse(dbmd.isReadOnly());
            assertSame(dbmd, connection.getMetaData());
            connection.close();
            assertThat(assertThrows(SQLException.class, connection::getMetaData).getMessage(), containsString("closed"));
        }
    }

    @Test
    void willDefaultToConnectionToNotBeCachedWhenNoConnectionParamIsPassedInUrl() throws SQLException {
        try (FireboltConnection connection = createConnection(SYSTEM_ENGINE_URL, connectionProperties)) {
            assertFalse(connection.isConnectionCachingEnabled());
        }
    }

    @ParameterizedTest
    @CsvSource(value = {
            "true",
            "false" })
    void willNotRespectTheCacheConnectionParameterAndDefaultToAlwaysNotCacheTheConnection(String actualValue) throws SQLException {
        // append to the system engine url the cache parameter
        String urlWithCacheConnection = SYSTEM_ENGINE_URL + "&cache_connection=" + actualValue;
        try (FireboltConnection connection = createConnection(urlWithCacheConnection, connectionProperties)) {
            assertFalse(connection.isConnectionCachingEnabled());
        }
    }

    protected FireboltConnection createConnection(String url, Properties props) throws SQLException {
        return new FireboltConnectionUserPassword(url, props, fireboltAuthenticationService, fireboltStatementService,
                fireboltEngineService, ParserVersion.LEGACY);
    }
}
