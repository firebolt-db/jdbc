package com.firebolt.jdbc.connection;

import com.firebolt.jdbc.exception.FireboltException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
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

    @ParameterizedTest
    @ValueSource(strings = {
            "jdbc:firebolt://api.firebolt.io/?env=dev&account=dev", // version 1
            "jdbc:firebolt:?env=dev&account=dev"  // version 2
    })
    void noEngineAndDb(String url) throws SQLException {
        assertThrows(FireboltException.class, () -> createConnection(url, connectionProperties));
    }

    @Test
    void notExistingDb() throws SQLException {
        connectionProperties.put("database", "my_db");
        when(fireboltGatewayUrlService.getUrl(any(), any())).thenReturn("http://my_endpoint");
        when(fireboltEngineService.doesDatabaseExist("my_db")).thenReturn(false);
        assertEquals("Database my_db does not exist", assertThrows(FireboltException.class, () -> createConnection("jdbc:firebolt:?env=dev&account=dev", connectionProperties)).getMessage());
    }

    @Test
    void getSystemEngineMetadata() throws SQLException {
        assertMetadata("", true);
    }

    protected FireboltConnection createConnection(String url, Properties props) throws SQLException {
        return new FireboltConnectionServiceSecret(url, props, fireboltAuthenticationService, fireboltGatewayUrlService, fireboltStatementService, fireboltEngineService, fireboltAccountIdService);
    }
}
