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

    private String secondEngineName;
    private String secondDbName;

    @BeforeAll
    void beforeAll() throws SQLException {
        executeStatementFromFile("/statements/cached-connection/ddl.sql");

        // create the engine and db here rather than the ddl so we can add the prefix to the engine and db names
        String engine = System.getProperty("engine");
        String db = System.getProperty("db");

        secondEngineName = engine + "_second_engine";
        secondDbName = db + "_second_db";

        try (Connection connection = createConnection()) {
            connection.createStatement().execute("CREATE ENGINE IF NOT EXISTS "  + secondEngineName +";");
            connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS " + secondDbName + ";");
        }

    }

    @AfterAll
    void afterEach() throws SQLException {
        try (Connection connection = createConnection()) {
            connection.createStatement().execute("STOP ENGINE " + secondEngineName + " WITH TERMINATE=true;");
            connection.createStatement().execute("DROP ENGINE IF EXISTS " + secondEngineName+ ";");
            connection.createStatement().execute("DROP DATABASE IF EXISTS " + secondDbName + ";");
        }

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
        try (Connection connection = createConnection(secondEngineName, secondDbName)) {
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
            String queryHistoryQueryFormat =
				"SELECT query_text " +
				"FROM information_schema.engine_query_history " +
				"WHERE submitted_time > '%s' and status = 'STARTED_EXECUTION' and (query_text='SELECT 101;' or query_text='SELECT 103;') " +
				"order by submitted_time desc";
            ResultSet engineOneResultSet = connection.createStatement().executeQuery(String.format(queryHistoryQueryFormat, testStartTime));
            assertTrue(engineOneResultSet.next());
            assertEquals("SELECT 103;", engineOneResultSet.getString(1));

            assertTrue(engineOneResultSet.next());
            assertEquals("SELECT 101;", engineOneResultSet.getString(1));

            assertFalse(engineOneResultSet.next());

        }

        // create a connection on the second engine and database
        try (Connection connection = createConnection(secondEngineName, secondDbName)) {
            ResultSet rs = connection.createStatement().executeQuery("SELECT 104");
            assertTrue(rs.next());

            // wait for the query history to propagate
            sleepForMillis(10000);

            // check that the connection cached was using the correct connection to the db and engine
            String queryHistoryQueryFormat =
				"SELECT query_text " +
				"FROM information_schema.engine_query_history " +
				"WHERE submitted_time > '%s' and status = 'STARTED_EXECUTION' and (query_text='SELECT 102;' or query_text='SELECT 104;') " +
				"order by submitted_time desc";

            ResultSet engineTwoResultSet = connection.createStatement().executeQuery(String.format(queryHistoryQueryFormat, testStartTime));
            assertTrue(engineTwoResultSet.next());
            assertEquals("SELECT 104;", engineTwoResultSet.getString(1));

            assertTrue(engineTwoResultSet.next());
            assertEquals("SELECT 102;", engineTwoResultSet.getString(1));

            assertFalse(engineTwoResultSet.next());


        }


    }

}
