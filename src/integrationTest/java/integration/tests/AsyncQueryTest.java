package integration.tests;

import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.statement.FireboltStatement;
import integration.IntegrationTest;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@CustomLog
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AsyncQueryTest extends IntegrationTest {
    String engineName;

    @AfterEach
    void afterEach() {
        try (Connection connection = createConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate("DELETE FROM async_table_test");
        } catch (Exception e) {
            fail("Could not execute statement", e);
        }
    }

    @BeforeAll
    void beforeAll() {
        engineName = System.getProperty("engine") + "async_test_second_engine";

        try {
            executeStatementFromFile("/statements/async/ddl.sql");
            try (FireboltConnection connection = createConnection().unwrap(FireboltConnection.class)) {
                connection.createStatement().execute("CREATE ENGINE IF NOT EXISTS " + engineName + ";");
            }

        } catch (Exception e) {
            fail("Could not execute statement", e);
        }
    }

    @AfterAll
    void afterAll() {
        try {
            try (FireboltConnection connection = createConnection().unwrap(FireboltConnection.class)) {
                connection.createStatement().execute("STOP ENGINE " + engineName + " WITH TERMINATE=true;");
                connection.createStatement().execute("DROP ENGINE IF EXISTS " + engineName + ";");
            }

            executeStatementFromFile("/statements/async/cleanup.sql");
        } catch (Exception e) {
            fail("Could not execute statement", e);
        }
    }

    @Test
    @Tag("v2")
    void executeServerSideAsyncTest() throws SQLException {
        try (FireboltConnection connection = createConnection().unwrap(FireboltConnection.class);
             FireboltStatement statement = connection.createStatement().unwrap(FireboltStatement.class)) {
            statement.executeAsync("INSERT INTO async_table_test SELECT checksum(*) FROM GENERATE_SERIES(1, 2500000000)");
            String token = statement.getAsyncToken();

            assertNotNull(token);
            assertTrue(StringUtils.isNotEmpty(token));
            assertTrue(connection.isAsyncQueryRunning(token));

            sleepForMillis(TimeUnit.SECONDS.toMillis(5));

            assertFalse(connection.isAsyncQueryRunning(token));
            //these assertions are here to see that the token can be used multiple times
            assertFalse(connection.isAsyncQueryRunning(token));
            assertTrue(connection.isAsyncQuerySuccessful(token));

            assertTrue(connection.isAsyncQuerySuccessful(token));

            try (ResultSet resultSet = statement.executeQuery("SELECT count(*) FROM async_table_test")) {
                assertTrue(resultSet.next());
                assertEquals(1, resultSet.getInt(1));
            }
        }
    }

    @Test
    @Tag("v2")
    void executeServerSideAsyncTestFetchStatusWorksOnDifferentEngines() throws SQLException {
        try (FireboltConnection connection = createConnection().unwrap(FireboltConnection.class);
             FireboltStatement statement = connection.createStatement().unwrap(FireboltStatement.class)) {
            statement.executeAsync("INSERT INTO async_table_test SELECT checksum(*) FROM GENERATE_SERIES(1, 2500000000)");
            String token = statement.getAsyncToken();

            assertNotNull(token);
            assertTrue(StringUtils.isNotEmpty(token));
            assertTrue(connection.isAsyncQueryRunning(token));

            statement.execute(format("USE ENGINE %s", engineName));
            assertEquals(connection.getSessionProperties().getEngine(), engineName);

            assertTrue(connection.isAsyncQueryRunning(token));

            sleepForMillis(TimeUnit.SECONDS.toMillis(5));

            assertFalse(connection.isAsyncQueryRunning(token));
            assertTrue(connection.isAsyncQuerySuccessful(token));

            try (ResultSet resultSet = statement.executeQuery("SELECT count(*) FROM async_table_test")) {
                assertTrue(resultSet.next());
                assertEquals(1, resultSet.getInt(1));
            }
        }
    }

    @Test
    @Tag("v2")
    void executeCancelServerSideAsyncQueryTest() throws SQLException {
        try (FireboltConnection connection = createConnection().unwrap(FireboltConnection.class);
             FireboltStatement statement = connection.createStatement().unwrap(FireboltStatement.class)) {
            statement.executeAsync("INSERT INTO async_table_test SELECT checksum(*) FROM GENERATE_SERIES(1, 2500000000)");
            String token = statement.getAsyncToken();

            assertNotNull(token);
            assertTrue(StringUtils.isNotEmpty(token));

            sleepForMillis(TimeUnit.SECONDS.toMillis(1));

            assertTrue(connection.isAsyncQueryRunning(token));
            assertTrue(connection.cancelAsyncQuery(token));
            assertFalse(connection.isAsyncQueryRunning(token));

            try (ResultSet resultSet = statement.executeQuery("SELECT count(*) FROM async_table_test")) {
                assertTrue(resultSet.next());
                assertEquals(0, resultSet.getInt(1));
            }
        }
    }

    @Test
    @Tag("v2")
    void executeCancelServerSideAsyncQueryWorksOnDifferentEngines() throws SQLException {
        try (FireboltConnection connection = createConnection().unwrap(FireboltConnection.class);
             FireboltStatement statement = connection.createStatement().unwrap(FireboltStatement.class)) {
            statement.executeAsync("INSERT INTO async_table_test SELECT checksum(*) FROM GENERATE_SERIES(1, 2500000000)");
            String token = statement.getAsyncToken();

            assertNotNull(token);
            assertTrue(StringUtils.isNotEmpty(token));
            assertTrue(connection.isAsyncQueryRunning(token));

            statement.execute(format("USE ENGINE %s", engineName));
            assertEquals(connection.getSessionProperties().getEngine(), engineName);

            assertTrue(connection.isAsyncQueryRunning(token));

            FireboltException exception = assertThrows(FireboltException.class, () -> connection.cancelAsyncQuery(token));
            assertTrue(exception.getMessage().contains("Attempt to cancel query with query_id"));

            assertTrue(connection.isAsyncQueryRunning(token));

            try (ResultSet resultSet = statement.executeQuery("SELECT count(*) FROM async_table_test")) {
                assertTrue(resultSet.next());
                assertEquals(0, resultSet.getInt(1));
            }
        }
    }

    @Test
    @Tag("v2")
    void executeCancelServerSideAsyncQueryAfterQueryHasFinishedTest() throws SQLException {
        try (FireboltConnection connection = createConnection().unwrap(FireboltConnection.class);
             FireboltStatement statement = connection.createStatement().unwrap(FireboltStatement.class)) {
            statement.executeAsync("INSERT INTO async_table_test SELECT checksum(*) FROM GENERATE_SERIES(1, 250000000)");
            String token = statement.getAsyncToken();

            assertNotNull(token);
            assertTrue(StringUtils.isNotEmpty(token));

            sleepForMillis(TimeUnit.SECONDS.toMillis(1));

            assertFalse(connection.isAsyncQueryRunning(token));

            FireboltException exception = assertThrows(FireboltException.class, () -> connection.cancelAsyncQuery(token));
            assertTrue(exception.getMessage().contains("Attempt to cancel query with query_id"));

            assertFalse(connection.isAsyncQueryRunning(token));

            try (ResultSet resultSet = statement.executeQuery("SELECT count(*) FROM async_table_test")) {
                assertTrue(resultSet.next());
                assertEquals(1, resultSet.getInt(1));
            }
        }
    }

    @Test
    @Tag("v2")
    void executeCancelServerSideAsyncQueryWithNonExistentTokenTest() throws SQLException {
        try (FireboltConnection connection = createConnection().unwrap(FireboltConnection.class)) {

            FireboltException exception = assertThrows(FireboltException.class, () -> connection.cancelAsyncQuery("random_token"));
            assertTrue(exception.getMessage().contains("Invalid async token"));
        }
    }
}
