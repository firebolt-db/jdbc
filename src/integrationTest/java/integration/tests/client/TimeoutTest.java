package integration.tests.client;

import com.firebolt.jdbc.connection.FireboltConnection;
import integration.IntegrationTest;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

public class TimeoutTest extends IntegrationTest {

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
    public void shouldNotTimeout() throws SQLException {
        mockBackEnd.enqueue(new MockResponse().setBodyDelay(300, TimeUnit.SECONDS)
                .setChunkedBody("", 1)
                .setResponseCode(200));

        try (FireboltConnection fireboltConnection = (FireboltConnection) createLocalConnection(String.format("?ssl=0&port=%d", mockBackEnd.getPort())); Statement statement = fireboltConnection.createStatement()) {
            ResultSet rs = statement.executeQuery("SELECT 1;");
        }
    }
}