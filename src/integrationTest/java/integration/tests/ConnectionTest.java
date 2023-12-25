package integration.tests;

import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.exception.ExceptionType;
import com.firebolt.jdbc.exception.FireboltException;
import integration.ConnectionInfo;
import integration.InfraVersion;
import integration.IntegrationTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ConnectionTest extends IntegrationTest {
    private int infraVersion;

    enum EngineType {
        SYSTEM_ENGINE, CUSTOM_ENGINE
    }

    @BeforeAll
    void beforeAll() throws SQLException {
        try (Connection conn = createConnection()) {
            infraVersion = ((FireboltConnection) conn).getInfraVersion();
        }
    }

    @ParameterizedTest(name = "{0}")
    @Tag("v2")
    @EnumSource(EngineType.class)
    void connectToNotExistingDb(EngineType engineType) {
        String database = "wrong_db";
        if (infraVersion >= 2) {
            assumeTrue(EngineType.CUSTOM_ENGINE.equals(engineType));
        }
        ConnectionInfo params = integration.ConnectionInfo.getInstance();
        String engineSuffix = EngineType.CUSTOM_ENGINE.equals(engineType) ? "&engine=" + params.getEngine() : "";
        String url = format("jdbc:firebolt:%s?env=%s&account=%s%s", database, params.getEnv(), params.getAccount(), engineSuffix);
        FireboltException e = assertThrows(FireboltException.class, () -> DriverManager.getConnection(url, params.getPrincipal(), params.getSecret()));
        if (infraVersion >= 2) {
            assertEquals(ExceptionType.ERROR, e.getType());
            String expectedMessage = format("Database '%s' does not exist or not authorized", database);
            assertTrue(e.getMessage().contains(expectedMessage), format("Error message '%s' does not match '%s'", e.getMessage(), expectedMessage));
        } else {
            assertEquals(ExceptionType.RESOURCE_NOT_FOUND, e.getType());
            assertEquals(format("Database %s does not exist", database), e.getMessage());
        }
    }

    /**
     * Try to connect to existing DB and existing engine but the engine is attached to another DB.
     * @throws SQLException if connection fails
     */
    @Test
    @Tag("v2")
    @InfraVersion(value = 2, comparison = InfraVersion.Comparison.LT)
    void connectToWrongDbNotAttachedToEngine() throws SQLException {
        ConnectionInfo params = integration.ConnectionInfo.getInstance();
        String enginelessDb = "engineless_db" + System.currentTimeMillis();
        try (Connection systemConnection = createConnection(null)) {
            try {
                systemConnection.createStatement().executeUpdate(format("CREATE DATABASE IF NOT EXISTS %s", enginelessDb));
                String url = format("jdbc:firebolt:%s?env=%s&account=%s&engine=%s", enginelessDb, params.getEnv(), params.getAccount(), params.getEngine());
                String errorMessage = format("The engine with the name %s is not attached to database %s", params.getEngine(), enginelessDb);
                assertEquals(errorMessage, assertThrows(FireboltException.class, () -> DriverManager.getConnection(url, params.getPrincipal(), params.getSecret())).getMessage());
            } finally {
                systemConnection.createStatement().executeUpdate(format("DROP DATABASE IF EXISTS %s", enginelessDb));
            }
        }
    }

    @ParameterizedTest
    @CsvSource({
            "false, false",
            "true, false",
            "false, true",
            "true, true"
    })
    void connect(boolean useDatabase, boolean useEngine) throws SQLException {
        ConnectionInfo params = integration.ConnectionInfo.getInstance();
        String database = useDatabase ? params.getDatabase() : null;
        String engine = useEngine ? params.getEngine() : null;
        ConnectionInfo updated = new ConnectionInfo(params.getPrincipal(), params.getSecret(), params.getEnv(), database, params.getAccount(), engine, params.getApi());
        String url = updated.toJdbcUrl();
        String query = "SELECT TOP 1 * FROM information_schema.tables";
        boolean expectedSuccess = updated.getApi() != null || infraVersion == 2 ? useDatabase : useDatabase || useEngine;
        if (expectedSuccess) {
            try (Connection connection = DriverManager.getConnection(url, params.getPrincipal(), params.getSecret());
                 Statement statement = connection.createStatement()) {
                assertNotNull(connection);
                ResultSet rs = statement.executeQuery(query);
                int n = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    for (int i = 1; i <= n; i++) {
                        rs.getObject(1);
                    }
                }
            }
        } else {
            assertThrows(FireboltException.class, () -> DriverManager.getConnection(url, params.getPrincipal(), params.getSecret()));
        }
    }
}
