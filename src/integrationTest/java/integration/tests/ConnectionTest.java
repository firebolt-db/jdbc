package integration.tests;

import com.firebolt.jdbc.connection.CacheListener;
import com.firebolt.jdbc.exception.ExceptionType;
import com.firebolt.jdbc.exception.FireboltException;
import integration.ConnectionInfo;
import integration.EnvironmentCondition;
import integration.IntegrationTest;
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
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class ConnectionTest extends IntegrationTest {

    enum EngineType {
        SYSTEM_ENGINE, CUSTOM_ENGINE
    }

    @ParameterizedTest(name = "{0}")
    @Tag("v2")
    @EnumSource(EngineType.class)
    void connectToNotExistingDb(EngineType engineType) {
        String database = "wrong_db";
        assumeTrue(EngineType.CUSTOM_ENGINE.equals(engineType));
        ConnectionInfo params = integration.ConnectionInfo.getInstance();
        String engineSuffix = EngineType.CUSTOM_ENGINE.equals(engineType) ? "&engine=" + params.getEngine() : "";
        String url = format("jdbc:firebolt:%s?env=%s&account=%s%s", database, params.getEnv(), params.getAccount(), engineSuffix);
        FireboltException e = assertThrows(FireboltException.class, () -> DriverManager.getConnection(url, params.getPrincipal(), params.getSecret()));
        assertEquals(ExceptionType.INVALID_REQUEST, e.getType());
        String expectedMessage = format("Database '%s' does not exist or not authorized", database);
        assertTrue(e.getMessage().contains(expectedMessage), format("Error message '%s' does not match '%s'", e.getMessage(), expectedMessage));
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
    @Tag("v2")
    @Tag("slow")
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
    @Tag("v2")
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
    @Tag("v2")
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
    @Tag("v2")
    void networkPolicyBlockedServiceAccountThrowsError() throws SQLException {
        try (Connection connection = createConnection();
             Statement statement = connection.createStatement()) {
            //this is done as to not clutter the environment variables for one test
            ConnectionInfo connectionInfo = getConnectionInfoForNetworkPolicyTest(statement);
            String jdbcUrl = getJdbcUrl(connectionInfo, true, true);

            //this should fail when executing setting the database
            FireboltException exception = assertThrows(FireboltException.class,
                    () -> DriverManager.getConnection(jdbcUrl, connectionInfo.getPrincipal(), connectionInfo.getSecret()));
            assertTrue(exception.getMessage().contains("Unauthorized"));

            // This is a workaround for the fact that the engine url is memory cached from the other tests
            ((CacheListener)connection).cleanup();

            // This should fail when getting the system engine url with a more specific error message
            ConnectionInfo connectionInfo2 = getConnectionInfoForNetworkPolicyTest(statement);
            exception = assertThrows(FireboltException.class,
                    () -> DriverManager.getConnection(jdbcUrl, connectionInfo2.getPrincipal(), connectionInfo2.getSecret()));
            assertTrue(exception.getMessage().contains("network restrictions"));

        }
    }

    private ConnectionInfo getConnectionInfoForNetworkPolicyTest(Statement statement) throws SQLException {
        ResultSet resultSet = statement
                .executeQuery("CALL fb_GENERATESERVICEACCOUNTKEY('network_policy_test_sa')");
        if (!resultSet.next()) {
            throw new FireboltException("Network Policy Test could now generate service account secret and id");
        }
        return new ConnectionInfo(resultSet.getString("service_account_id"),
                resultSet.getString("secret"), ConnectionInfo.getInstance().getEnv(),
                ConnectionInfo.getInstance().getDatabase(),
                ConnectionInfo.getInstance().getAccount(),
                ConnectionInfo.getInstance().getEngine(),
                ConnectionInfo.getInstance().getApi());
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
