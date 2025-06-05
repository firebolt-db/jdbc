package integration.tests;

import com.firebolt.jdbc.connection.CacheListener;
import com.firebolt.jdbc.exception.ExceptionType;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.testutils.TestTag;
import integration.ConnectionInfo;
import integration.ConnectionOptions;
import integration.EnvironmentCondition;
import integration.IntegrationTest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class ConnectionTest extends IntegrationTest {

    enum EngineType {
        CUSTOM_ENGINE, SYSTEM_ENGINE
    }

    @ParameterizedTest(name = "{0}")
    @Tag(TestTag.V2)
    @EnumSource(EngineType.class)
    void connectToNotExistingDbV2(EngineType engineType) throws SQLException {
        String database = "wrong_db";
        boolean useCustomEngine = EngineType.CUSTOM_ENGINE.equals(engineType);
        ConnectionInfo params = ConnectionInfo.getInstance();
        String url = getConnectionInfoWithEngineAndDatabase(useCustomEngine ? params.getEngine() : null, database).toJdbcUrl();
        FireboltException e;
        if (useCustomEngine) {
            e = assertThrows(FireboltException.class, () -> DriverManager.getConnection(url, params.getPrincipal(), params.getSecret()));
        } else {
            // the connection will be successful because v2 does not set database or engine on system engine connection
            try (Connection connection = DriverManager.getConnection(url, params.getPrincipal(), params.getSecret());
                ResultSet resultSet = connection.createStatement().executeQuery("SELECT 1")) {
                assertTrue(resultSet.next());
                assertEquals(1, resultSet.getInt(1));
                //even on system engine, the database should be set, but since database does not exist, it should throw an error
                e = assertThrows(FireboltException.class, () -> connection.createStatement().executeUpdate("USE DATABASE " + database));
            }
        }
        assertEquals(ExceptionType.INVALID_REQUEST, e.getType());
        String expectedMessage = format("Database '%s' does not exist or not authorized", database);
        assertTrue(e.getMessage().contains(expectedMessage), format("Error message '%s' does not match '%s'", e.getMessage(), expectedMessage));
    }

    @ParameterizedTest(name = "{0}")
    @Tag(TestTag.V1)
    @EnumSource(EngineType.class)
    void connectToNotExistingDbV1(EngineType engineType) throws SQLException {
        String database = "wrong_db";
        boolean useCustomEngine = EngineType.CUSTOM_ENGINE.equals(engineType);
        ConnectionInfo params = ConnectionInfo.getInstance();
        String url = getConnectionInfoWithEngineAndDatabase(useCustomEngine ? params.getEngine() : null, database).toJdbcUrl();
        if (useCustomEngine) {
            // the connection will be successful because v1 does not check for the database existence on connection
            try (Connection connection = DriverManager.getConnection(url, params.getPrincipal(), params.getSecret())) {
                // if the engine is not attached to the database, it will throw an error
                assertThrows(SQLException.class, () -> connection.createStatement().executeQuery("SELECT 1"));
            }
        } else {
            FireboltException e = assertThrows(FireboltException.class, () -> DriverManager.getConnection(url, params.getPrincipal(), params.getSecret()));
            assertEquals(ExceptionType.RESOURCE_NOT_FOUND, e.getType());
            assertEquals(format("The database with the name %s could not be found", database), e.getMessage());
        }
    }

    @Test
    @Tag(TestTag.CORE)
    void connectToNotExistingDbInCore() {
        // when backend will fix their code this test will start failing
        String database = "wrong_db";
        FireboltException e = assertThrows(FireboltException.class, () -> createConnectionWithOptions(ConnectionOptions.builder().database(database).build()));
        assertEquals(ExceptionType.INVALID_REQUEST, e.getType());
        String expectedMessage = format("Database '%s' does not exist or not authorized", database);
        assertTrue(e.getMessage().contains(expectedMessage), format("Error message '%s' does not match '%s'", e.getMessage(), expectedMessage));
    }

    @Test
    @Tag(TestTag.CORE)
    void doNotHaveToSpecifyTheDatabaseWhenConnectingToCore() throws SQLException {
        try (Connection fireboltConnection = createConnectionWithOptions(ConnectionOptions.builder().database(null).build()); Statement statement = fireboltConnection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("SELECT 1");
            assertTrue(resultSet.next());
            assertEquals("1", resultSet.getString(1));
            assertFalse(resultSet.next());

            assertTrue(fireboltConnection.isValid((int) TimeUnit.MILLISECONDS.toMillis(500)));
        }
    }

    /**
     * Try to connect to existing DB and existing engine but the engine is attached to another DB.
     * @throws SQLException if connection fails
     */
    @Test
    @Tag(TestTag.V2)
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
    @Tag(TestTag.V1)
    void successfulConnectV1(boolean useDatabase, boolean useEngine) throws SQLException {
        successfulConnect(useDatabase, useEngine);
    }

    @ParameterizedTest(name = "using db:{0} engine:{1}")
    @CsvSource({
            "false, false",
    })
    @Tag(TestTag.V1)
    void unsuccessfulConnectV1(boolean useDatabase, boolean useEngine) throws SQLException {
        unsuccessfulConnect(useDatabase, useEngine);
    }

    @ParameterizedTest(name = "using db:{0} engine:{1}")
    @CsvSource({
            "false, true", // can connect but cannot execute select
    })
    @Tag(TestTag.V1)
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
    @Tag(TestTag.V2)
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
    @Tag(TestTag.V2)
    @Tag(TestTag.SLOW)
    void successfulConnectWithoutStartingTheEngine() throws SQLException {
        String currentUTCTime = getCurrentUTCTime();

        ConnectionInfo params = integration.ConnectionInfo.getInstance();
        String url = getJdbcUrl(params, true, true);
        try (Connection connection = DriverManager.getConnection(url, params.getPrincipal(), params.getSecret());
             Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery("SELECT 222");
            assertTrue(rs.next());
            assertNotNull(rs.getObject(1));

            // wait for the query_history to propagate
            sleepForMillis(TimeUnit.SECONDS.toMillis(10));

            // there should be no select 1 statement executed
            String selectOneQueryHistoryQueryFormat =
				"SELECT query_text " +
				"FROM information_schema.engine_query_history WHERE submitted_time > '%s' and (query_text like '%%SELECT 1%%' OR query_text like '%%select 1%%');";
            ResultSet selectQueriesRS = statement.executeQuery(String.format(selectOneQueryHistoryQueryFormat, currentUTCTime));
            assertFalse(selectQueriesRS.next());

            // there should be one select 222 statement executed
            String queryHistoryQueryFormat =
				"SELECT query_text " +
				"FROM information_schema.engine_query_history WHERE submitted_time > '%s' and query_text like 'SELECT 222%%' and status = 'STARTED_EXECUTION';";

            selectQueriesRS = statement.executeQuery(String.format(queryHistoryQueryFormat, currentUTCTime));
            assertTrue(selectQueriesRS.next());

            assertEquals("SELECT 222;", selectQueriesRS.getString(1));

            // no more select statements
            assertFalse(selectQueriesRS.next());
        }
    }

    @Test
    @Tag(TestTag.V2)
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

    @Test
    @Tag(TestTag.V2)
    @Tag(TestTag.CORE)
    void preparedStatementBatchesWorkIfMergeParameterProvided() throws SQLException {
        String engineName = integration.ConnectionInfo.getInstance().getEngine();
        String queryLabel = "test_merge_batches_" + System.currentTimeMillis();
        try (Connection connection = createConnection(engineName, Map.of("merge_prepared_statement_batches", "true"))) {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("CREATE TABLE test_table (id INT)");
                try (java.sql.PreparedStatement preparedStatement = connection.prepareStatement(
                        String.format("/*%s*/INSERT INTO test_table VALUES (?)", queryLabel))) {
                    for (int i = 0; i < 10; i++) {
                        preparedStatement.setInt(1, i);
                        preparedStatement.addBatch();
                    }
                    preparedStatement.executeBatch();

                }
                try (ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM test_table")) {
                    assertTrue(rs.next());
                    assertEquals(10, rs.getInt(1));
                }
                // sleep for 10s to give QH time to get populated and avoid flakiness
                // it sometime takes that long
                sleepForMillis(10000);

                // Validate we've only executed one insert
                String qhQuery = "SELECT count(*) from information_schema.engine_query_history WHERE status='ENDED_SUCCESSFULLY' " +
                        String.format("AND lower(query_text) like '/*%s*/insert into %%'", queryLabel);
                System.out.println(qhQuery);
                try (java.sql.PreparedStatement preparedStatement = connection.prepareStatement(qhQuery)) {
                    try (ResultSet rs = preparedStatement.executeQuery()) {
                        assertTrue(rs.next());
                        assertEquals(1, rs.getInt(1));
                    }
                }

            } finally {
                try (Statement statement = connection.createStatement()) {
                    statement.executeUpdate("DROP TABLE IF EXISTS test_table");
                }
            }
        }
    }

    @Test
    @Tag(TestTag.V2)
    void networkPolicyBlockedServiceAccountThrowsError() throws SQLException {
        try (Connection connection = createConnection();
             Statement statement = connection.createStatement()) {
            //this is done as to not clutter the environment variables for one test
            ConnectionInfo connectionInfo = getConnectionInfoForNetworkPolicyTest(statement);
            String jdbcUrl = connectionInfo.toJdbcUrl();

            //this should fail when executing setting the database
            FireboltException exception = assertThrows(FireboltException.class,
                    () -> DriverManager.getConnection(jdbcUrl, connectionInfo.getPrincipal(), connectionInfo.getSecret()));
            assertTrue(exception.getMessage().contains("Unauthorized"));

            // This is a workaround for the fact that the engine url is memory cached from the other tests
            ((CacheListener)connection).cleanup();

            // This should fail when getting the system engine url with a more specific error message
            exception = assertThrows(FireboltException.class,
                    () -> DriverManager.getConnection(jdbcUrl, connectionInfo.getPrincipal(), connectionInfo.getSecret()));
            assertTrue(exception.getMessage().contains("network restrictions"));

        }
    }

    @ParameterizedTest
    @CsvSource(value = {
            "http://172.0.1.280,Invalid URL format. URL must be in the form: <protocol>://<host>:<port>. You must specify the port.",
            "https://172.0.1.280,Invalid URL format. URL must be in the form: <protocol>://<host>:<port>. You must specify the port.",
            "http://172.0.0.1:,Invalid URL format. URL must be in the form: <protocol>://<host>:<port>. You must specify the port.",
            "https://172.0.0.1:,Invalid URL format. URL must be in the form: <protocol>://<host>:<port>. You must specify the port.",
            "http://172.0.0.1:-2,Invalid URL format. URL must be in the form: <protocol>://<host>:<port>. Invalid port number :-2",
            "https://172.0.0.1:-2,Invalid URL format. URL must be in the form: <protocol>://<host>:<port>. Invalid port number :-2",
            "http://172.0.0.1:70000,Invalid URL format. URL must be in the form: <protocol>://<host>:<port>. The port value should be a positive integer between 1 and 65535. You have the port as:70000",
            "https://172.0.0.1:70000,Invalid URL format. URL must be in the form: <protocol>://<host>:<port>. The port value should be a positive integer between 1 and 65535. You have the port as:70000",
            "localhost:8080,Invalid URL format. URL must be in the form: <protocol>://<host>:<port>. You did not pass in the protocol. It has to be either http or https.",
            "http://mydomain.com,Invalid URL format. URL must be in the form: <protocol>://<host>:<port>. You must specify the port.",
            "https://mydomain.com,Invalid URL format. URL must be in the form: <protocol>://<host>:<port>. You must specify the port."
    }, delimiter = ',')
    @Tag(TestTag.CORE)
    void cannotConnectToFireboltCoreWithInvalidUrl(String invalidUrl, String expectedErrorMessage) {
        SQLException exception = assertThrows(SQLException.class, () -> createConnectionWithOptions(ConnectionOptions.builder().url(invalidUrl).build()));
        assertEquals(expectedErrorMessage, exception.getMessage());
    }

    @Test
    @Tag(TestTag.CORE)
    void canConnectToFirebotlCoreOverHttps() throws SQLException {
        try (Connection connection = createConnectionWithOptions(ConnectionOptions.builder().url("https://localhost:443").build()); Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("SELECT 1");
            assertTrue(resultSet.next());
            assertEquals(1, resultSet.getInt(1));

            assertTrue(connection.isValid((int)TimeUnit.MILLISECONDS.toMillis(500)));
        }
    }

    private ConnectionInfo getConnectionInfoForNetworkPolicyTest(Statement statement) throws SQLException {
        ResultSet resultSet = statement
                .executeQuery("CALL fb_GENERATESERVICEACCOUNTKEY('network_policy_test_sa')");
        if (!resultSet.next()) {
            throw new FireboltException("Network Policy Test could not generate service account secret and id");
        }
        return new ConnectionInfo(resultSet.getString("service_account_id"),
                resultSet.getString("secret"), ConnectionInfo.getInstance().getEnv(),
                ConnectionInfo.getInstance().getDatabase(),
                ConnectionInfo.getInstance().getAccount(),
                ConnectionInfo.getInstance().getEngine(),
                ConnectionInfo.getInstance().getApi(),
                Map.of("cache_connection", "false"));
    }

    void unsuccessfulConnect(boolean useDatabase, boolean useEngine) {
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
