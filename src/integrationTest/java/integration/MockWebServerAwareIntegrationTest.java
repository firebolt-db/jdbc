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
    private final int INIT_STATEMENTS_COUNT = 1; // The statement that validates that DB exists.

    @BeforeEach
    void setUp() throws IOException {
        mockBackEnd = new MockWebServer();
        mockBackEnd.start();
        mockBackEnd.enqueue(new MockResponse().setResponseCode(200).setBody(format("database_name%nstring%n%s", System.getProperty("db"))));
    }

    @AfterEach
    void tearDown() throws IOException {
        mockBackEnd.close();
    }


    protected void assertMockBackendRequestsCount(int expected) {
        assertEquals(INIT_STATEMENTS_COUNT + expected, mockBackEnd.getRequestCount());
    }
}
