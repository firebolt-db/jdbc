package com.firebolt.jdbc.connection;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Properties;

import static com.firebolt.jdbc.connection.FireboltConnectionUserPassword.SYSTEM_ENGINE_NAME;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class FireboltConnectionUserPasswordTest extends FireboltConnectionTest {
    private static final String SYSTEM_ENGINE_URL = "jdbc:firebolt:db?env=dev&account=dev&engine=system";

    public FireboltConnectionUserPasswordTest() {
        super("jdbc:firebolt://api.dev.firebolt.io/db");
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
        try (FireboltConnection connection = createConnection(SYSTEM_ENGINE_URL, connectionProperties)) {
            verify(fireboltEngineService, times(1)).getEngine(argThat(props -> "my_db".equals(props.getDatabase()) && SYSTEM_ENGINE_NAME.equals(props.getEngine())));
            assertEquals("endpoint", connection.getSessionProperties().getHost());
        }
    }


    @Test
    void noEngineAndDb() throws SQLException {
        try (FireboltConnection connection = createConnection("jdbc:firebolt:?env=dev&account=dev", connectionProperties)) {
            assertEquals("endpoint", connection.getSessionProperties().getHost());
            assertNotNull(connection.getSessionProperties().getEngine());
            assertFalse(connection.getSessionProperties().isSystemEngine());
        }
    }

    @ParameterizedTest
    @CsvSource({
            "eng,false",
            "system,true" // system engine is readonly
    })
    void getMetadata(String engine, boolean readOnly) throws SQLException {
        try (FireboltConnection connection = createConnection(format("jdbc:firebolt:db?env=dev&engine=%s&account=dev", engine), connectionProperties)) {
            DatabaseMetaData dbmd = connection.getMetaData();
            assertEquals(readOnly, dbmd.isReadOnly());
        }
    }

    protected FireboltConnection createConnection(String url, Properties props) throws SQLException {
        return new FireboltConnectionUserPassword(url, props, fireboltAuthenticationService, fireboltStatementService, fireboltEngineService);
    }
}
