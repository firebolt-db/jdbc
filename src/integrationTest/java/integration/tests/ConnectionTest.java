package integration.tests;

import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.exception.ExceptionType;
import com.firebolt.jdbc.exception.FireboltException;
import integration.ConnectionInfo;
import integration.EnvironmentCondition;
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
import java.util.Map;

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
            assertEquals(ExceptionType.INVALID_REQUEST, e.getType());
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
    @EnvironmentCondition(value = "2", comparison = EnvironmentCondition.Comparison.LT)
    void connectToWrongDbNotAttachedToEngine() throws SQLException {
        ConnectionInfo params = integration.ConnectionInfo.getInstance();
        String enginelessDb = "engineless_db" + System.currentTimeMillis();
        try (Connection systemConnection = createConnection(null)) {
            try {
                systemConnection.createStatement().executeUpdate(format("CREATE DATABASE IF NOT EXISTS \"%s\"", enginelessDb));
                String url = format("jdbc:firebolt:%s?env=%s&account=%s&engine=%s", enginelessDb, params.getEnv(), params.getAccount(), params.getEngine());
                String errorMessage = format("The engine with the name %s is not attached to database %s", params.getEngine(), enginelessDb);
                assertEquals(errorMessage, assertThrows(FireboltException.class, () -> DriverManager.getConnection(url, params.getPrincipal(), params.getSecret())).getMessage());
            } finally {
                systemConnection.createStatement().executeUpdate(format("DROP DATABASE IF EXISTS %s", enginelessDb));
            }
        }
    }

    @ParameterizedTest(name = "using db:{0} engine:{1}")
    @CsvSource({
            "true, false",
            "true, true"
    })
    @Tag("v1")
    void successfulConnectV1(boolean useDatabase, boolean useEngine) throws SQLException {
        successfulConnect(useDatabase, useEngine);
    }

    @ParameterizedTest(name = "using db:{0} engine:{1}")
    @CsvSource({
            "false, false",
    })
    @Tag("v1")
    void unsuccessfulConnectV1(boolean useDatabase, boolean useEngine) throws SQLException {
        unsuccessfulConnect(useDatabase, useEngine);
    }

    @ParameterizedTest(name = "using db:{0} engine:{1}")
    @CsvSource({
            "false, true", // can connect but cannot execute select
    })
    @Tag("v1")
    void successfulConnectUnsuccessfulSelectV1(boolean useDatabase, boolean useEngine) throws SQLException {
        ConnectionInfo params = integration.ConnectionInfo.getInstance();
        String url = getJdbcUrl(params, useDatabase, useEngine);
        try (Connection connection = DriverManager.getConnection(url, params.getPrincipal(), params.getSecret());
             Statement statement = connection.createStatement()) {
            assertThrows(SQLException.class, () -> statement.executeQuery("SELECT 1"));
        }
    }

    @ParameterizedTest(name = "V2 using db:{0} engine:{1}")
    @CsvSource({
            "false, false",
            "false, true",
            "true, false",
            "true, true"

    })
    @Tag("v2")
    void successfulConnect(boolean useDatabase, boolean useEngine) throws SQLException {
        ConnectionInfo params = integration.ConnectionInfo.getInstance();
        String url = getJdbcUrl(params, useDatabase, useEngine);
        try (Connection connection = DriverManager.getConnection(url, params.getPrincipal(), params.getSecret());
             Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery("SELECT 1");
            assertTrue(rs.next());
            assertNotNull(rs.getObject(1));
        }
    }

    @Test
    void validatesOnSystemEngineIfParameterProvided() throws SQLException {
        try (Connection systemConnection = createConnection(null)) {
            String engineName = integration.ConnectionInfo.getInstance().getEngine() + "_validate_test";
            try (Statement systemStatement = systemConnection.createStatement()) {
                systemStatement.executeUpdate(format("CREATE ENGINE %s WITH INITIALLY_STOPPED=true", engineName));
            }
            try (Connection connection = createConnection(engineName, Map.of("validate_on_system_engine", "true"))) {
                try (Statement systemStatement = systemConnection.createStatement()) {
                    ResultSet rs = systemStatement.executeQuery(
                            format("SELECT status FROM information_schema.engines WHERE engine_name='%s'", engineName));
                    assertTrue(rs.next());
                    assertEquals("STOPPED", rs.getString(1));
                }
                assertTrue(connection.isValid(500));
                // After validation the engine should still be stopped
                try (Statement systemStatement = systemConnection.createStatement()) {
                    ResultSet rs = systemStatement.executeQuery(
                            format("SELECT status FROM information_schema.engines WHERE engine_name='%s'", engineName));
                    assertTrue(rs.next());
                    assertEquals("STOPPED", rs.getString(1));
                }
            } finally {
                try (Statement systemStatement = systemConnection.createStatement()) {
                    systemStatement.executeUpdate(format("DROP ENGINE %s", engineName));
                }
            }
        }

    }

    void unsuccessfulConnect(boolean useDatabase, boolean useEngine) throws SQLException {
        ConnectionInfo params = integration.ConnectionInfo.getInstance();
        String url = getJdbcUrl(params, useDatabase, useEngine);
        assertThrows(FireboltException.class, () -> DriverManager.getConnection(url, params.getPrincipal(), params.getSecret()));
    }

    private String getJdbcUrl(ConnectionInfo params, boolean useDatabase, boolean useEngine) {
        String database = useDatabase ? params.getDatabase() : null;
        String engine = useEngine ? params.getEngine() : null;
        ConnectionInfo updated = new ConnectionInfo(params.getPrincipal(), params.getSecret(), params.getEnv(), database, params.getAccount(), engine, params.getApi());
        return updated.toJdbcUrl();
    }
}
