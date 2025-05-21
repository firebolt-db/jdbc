package integration;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class MockWebServerAwareIntegrationTest extends CommonIntegrationTest {
    protected MockWebServer mockBackEnd;

    @BeforeEach
    void setUp() throws IOException {
        mockBackEnd = new MockWebServer();
        mockBackEnd.start();
    }

    @AfterEach
    void tearDown() throws IOException {
        mockBackEnd.close();
    }

    protected Connection createLocalConnection(String queryParams) throws SQLException {
        return DriverManager.getConnection(
                JDBC_URL_PREFIX + integration.ConnectionInfo.getInstance().getDatabase()
                        + queryParams + "&host=localhost" + getAccountParam(),
                integration.ConnectionInfo.getInstance().getPrincipal(),
                integration.ConnectionInfo.getInstance().getSecret());
    }

    private String getAccountParam() {
        return "&account=" + integration.ConnectionInfo.getInstance().getAccount();
    }

    protected void assertMockBackendRequestsCount(int expected) {
        assertEquals(expected, mockBackEnd.getRequestCount());
    }
}
