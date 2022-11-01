package integration.tests.client;

import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.exception.FireboltException;
import integration.IntegrationTest;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;

import static com.firebolt.jdbc.exception.ExceptionType.INVALID_REQUEST;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RetryPolicyTest extends IntegrationTest {

    private MockWebServer mockBackEnd;

    @BeforeEach
    void setUp() throws IOException {
        mockBackEnd = new MockWebServer();
        mockBackEnd.start();
    }

    @AfterEach
    void tearDown() throws IOException {
        mockBackEnd.close();
    }

    @Test
    public void shouldThrowExceptionOn400WithoutRetry() throws SQLException {
        mockBackEnd.enqueue(new MockResponse().setResponseCode(400));
        try (FireboltConnection fireboltConnection = (FireboltConnection) createLocalConnection(String.format("?ssl=0&port=%d&retries=%d", mockBackEnd.getPort(), 3)); Statement statement = fireboltConnection.createStatement()) {
            FireboltException ex = assertThrows(FireboltException.class, () -> statement.execute("SELECT 1;"));
            assertEquals(ex.getType(), INVALID_REQUEST);
            assertEquals(1, mockBackEnd.getRequestCount());
        }
    }

    @Test
    public void shouldRetryOn502() throws SQLException {
        mockBackEnd.enqueue(new MockResponse().setResponseCode(502));
        mockBackEnd.enqueue(new MockResponse().setResponseCode(502));
        mockBackEnd.enqueue(new MockResponse().setResponseCode(200));
        try (FireboltConnection fireboltConnection = (FireboltConnection) createLocalConnection(String.format("?ssl=0&port=%d&retries=%d", mockBackEnd.getPort(), 3)); Statement statement = fireboltConnection.createStatement()) {
            statement.execute("SELECT 1;");
            assertEquals(3, mockBackEnd.getRequestCount());
        }
    }

}
