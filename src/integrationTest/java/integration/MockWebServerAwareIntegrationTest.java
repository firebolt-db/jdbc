package integration;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;

import static java.lang.String.format;
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
