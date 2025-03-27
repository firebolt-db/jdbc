package integration.tests;

import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.statement.FireboltStatement;
import integration.IntegrationTest;
import lombok.CustomLog;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.*;

@CustomLog
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AsyncQueryTest extends IntegrationTest {
    String engineName = format("%s_async_engine_test", integration.ConnectionInfo.getInstance().getEngine());

    @BeforeAll
    void beforeAll() {
        try (Connection connection = createConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(format("CREATE ENGINE %s", engineName));
        } catch (Exception e) {
            fail("Could not execute statement", e);
        }
    }

    @AfterAll
    void afterAll() {
        try (Connection connection = createConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(format("STOP ENGINE %s WITH TERMINATE=true", engineName));
            statement.execute(format("DROP ENGINE IF EXISTS %s", engineName));
        } catch (Exception e) {
            fail("Could not execute statement", e);
        }
    }

    @BeforeEach
    void beforeEach() {
        try {
            executeStatementFromFile("/statements/async/ddl.sql");
        } catch (Exception e) {
            fail("Could not execute statement", e);
        }
    }

    @AfterEach
    void afterEach() {
        try {
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
            assertFalse(token.isEmpty());
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
            assertFalse(token.isEmpty());
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
            assertFalse(token.isEmpty());

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
            assertFalse(token.isEmpty());
            assertTrue(connection.isAsyncQueryRunning(token));

            statement.execute(format("USE ENGINE %s", engineName));
            assertEquals(connection.getSessionProperties().getEngine(), engineName);

            assertTrue(connection.isAsyncQueryRunning(token));

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
            assertFalse(token.isEmpty());

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
