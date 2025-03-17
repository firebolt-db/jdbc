package integration.tests;

import integration.IntegrationTest;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CachedConnectionTest extends IntegrationTest {

    private static final String CACHED_TEST_ENGINE_NAME = "cached_test_second_engine";
    private static final String CACHED_TEST_DATABASE_NAME = "cached_test_second_db";

    @BeforeAll
    void beforeAll() {
        executeStatementFromFile("/statements/cached-connection/ddl.sql");
    }

    @AfterAll
    void afterEach() {
        executeStatementFromFile("/statements/cached-connection/cleanup.sql");
    }

    @Test
    @Tag("v2")
    @Tag("slow")
    void createTwoConnections() throws SQLException {
        String testStartTime = getCurrentUTCTime();

        // create a connection on the first engine and database
        try (Connection connection = createConnection()) {
            connection.createStatement().execute("INSERT INTO statement_test(id) values (101);");
        }

        // create a connection on the second engine and database
        try (Connection connection = createConnection(CACHED_TEST_ENGINE_NAME, CACHED_TEST_DATABASE_NAME)) {
            connection.createStatement().execute("INSERT INTO statement_test_cached(id) values (102);");
        }

        // create a connection back on the first engine and database
        try (Connection connection = createConnection()) {
            connection.createStatement().execute("INSERT INTO statement_test(id) values (103);");

            ResultSet rs = connection.createStatement().executeQuery("SELECT id FROM statement_test order by id desc");
            assertTrue(rs.next());
            assertEquals(rs.getInt("id"), 103);
            assertTrue(rs.next());
            assertEquals(rs.getInt("id"), 101);
            assertFalse(rs.next());

            // wait for the query history to propagate. Checking the query history will make sure that the queries were executed on the same engine
            sleepForMillis(TimeUnit.SECONDS.toMillis(10));

            assertQueryFoundInHistory(connection.createStatement(), testStartTime, "INSERT INTO statement_test(id) values (101);");
            assertQueryFoundInHistory(connection.createStatement(), testStartTime, "INSERT INTO statement_test(id) values (103);");
        }

        // create a connection on the second engine and database
        try (Connection connection = createConnection(CACHED_TEST_ENGINE_NAME, CACHED_TEST_DATABASE_NAME)) {
            connection.createStatement().execute("INSERT INTO statement_test_cached(id) values (104);");

            ResultSet rs = connection.createStatement().executeQuery("SELECT id FROM statement_test_cached order by id desc");
            assertTrue(rs.next());
            assertEquals(rs.getInt("id"), 104);
            assertTrue(rs.next());
            assertEquals(rs.getInt("id"), 102);
            assertFalse(rs.next());

            // wait for the query history to propagate. Checking the query history will make sure that the queries were executed on the same engine
            sleepForMillis(TimeUnit.SECONDS.toMillis(10));

            assertQueryFoundInHistory(connection.createStatement(), testStartTime, "INSERT INTO statement_test_cached(id) values (102);");
            assertQueryFoundInHistory(connection.createStatement(), testStartTime, "INSERT INTO statement_test_cached(id) values (104);");
        }

    }
    private void assertQueryFoundInHistory(Statement statement, String afterTimestamp, String queryText) throws SQLException {
        String queryHistoryQuery = """
				SELECT query_label
				FROM information_schema.engine_query_history WHERE submitted_time > '%s' and query_text = '%s';
			""";
        ResultSet resultSet = statement.executeQuery(String.format(queryHistoryQuery, afterTimestamp, queryText));
        assertTrue(resultSet.next(), "Did not find any query in history");
    }

}
