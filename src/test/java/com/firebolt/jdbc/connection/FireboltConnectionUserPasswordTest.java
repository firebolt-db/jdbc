package com.firebolt.jdbc.connection;

import com.firebolt.jdbc.type.ParserVersion;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;

import static com.firebolt.jdbc.connection.FireboltConnectionUserPassword.SYSTEM_ENGINE_NAME;
import static com.firebolt.jdbc.connection.settings.FireboltSessionProperty.ACCESS_TOKEN;
import static com.firebolt.jdbc.connection.settings.FireboltSessionProperty.HOST;
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

    protected FireboltConnection createConnection(String url, Properties props) throws SQLException {
        return new FireboltConnectionUserPassword(url, props, fireboltAuthenticationService, fireboltStatementService,
                fireboltEngineService, ParserVersion.LEGACY);
    }
}
