package integration;

import java.io.IOException;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class MockWebServerAwareIntegrationTest extends IntegrationTest {
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


    protected void assertMockBackendRequestsCount(int expected) {
        assertEquals(expected, mockBackEnd.getRequestCount());
    }
}
