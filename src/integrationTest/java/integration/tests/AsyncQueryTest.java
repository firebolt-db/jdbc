package integration.tests;

import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.statement.FireboltStatement;
import integration.IntegrationTest;
import lombok.CustomLog;
import org.junit.jupiter.api.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@CustomLog
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AsyncQueryTest extends IntegrationTest {

    @BeforeAll
    void beforeAll() {
        try {
            executeStatementFromFile("/statements/async/ddl.sql", getSystemEngineName());
        } catch (Exception e) {
            log.warn("Could not execute statement", e);
        }
    }

    @AfterAll
    void afterAll() {
        try {
            executeStatementFromFile("/statements/async/cleanup.sql", getSystemEngineName());
        } catch (Exception e) {
            log.warn("Could not execute statement", e);
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
}
