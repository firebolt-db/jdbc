package integration.tests;

import integration.IntegrationTest;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
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
            ResultSet rs = connection.createStatement().executeQuery("SELECT 101");
            assertTrue(rs.next());
        }

        // create a connection on the second engine and database
        try (Connection connection = createConnection(CACHED_TEST_ENGINE_NAME, CACHED_TEST_DATABASE_NAME)) {
            ResultSet rs = connection.createStatement().executeQuery("SELECT 102");
            assertTrue(rs.next());
        }

        // create a connection back on the first engine and database
        try (Connection connection = createConnection()) {
            ResultSet rs = connection.createStatement().executeQuery("SELECT 103");
            assertTrue(rs.next());

            // wait for the query history to propagate
            sleepForMillis(10000);

            // check that the connection cached was using the correct connection to the db and engine
            String queryHistoryQueryFormat = """
				SELECT query_text
				FROM information_schema.engine_query_history
				WHERE submitted_time > '%s' and status = 'STARTED_EXECUTION' and (query_text='SELECT 101;' or query_text='SELECT 103;')
				order by submitted_time desc
			""";
            ResultSet engineOneResultSet = connection.createStatement().executeQuery(String.format(queryHistoryQueryFormat, testStartTime));
            assertTrue(engineOneResultSet.next());
            assertEquals("SELECT 103;", engineOneResultSet.getString(1));

            assertTrue(engineOneResultSet.next());
            assertEquals("SELECT 101;", engineOneResultSet.getString(1));

            assertFalse(engineOneResultSet.next());

        }

        // create a connection on the second engine and database
        try (Connection connection = createConnection(CACHED_TEST_ENGINE_NAME, CACHED_TEST_DATABASE_NAME)) {
            ResultSet rs = connection.createStatement().executeQuery("SELECT 104");
            assertTrue(rs.next());

            // wait for the query history to propagate
            sleepForMillis(10000);

            // check that the connection cached was using the correct connection to the db and engine
            String queryHistoryQueryFormat = """
				SELECT query_text
				FROM information_schema.engine_query_history
				WHERE submitted_time > '%s' and status = 'STARTED_EXECUTION' and (query_text='SELECT 102;' or query_text='SELECT 104;')
				order by submitted_time desc
			""";

            ResultSet engineTwoResultSet = connection.createStatement().executeQuery(String.format(queryHistoryQueryFormat, testStartTime));
            assertTrue(engineTwoResultSet.next());
            assertEquals("SELECT 104;", engineTwoResultSet.getString(1));

            assertTrue(engineTwoResultSet.next());
            assertEquals("SELECT 102;", engineTwoResultSet.getString(1));

            assertFalse(engineTwoResultSet.next());


        }


    }

}
