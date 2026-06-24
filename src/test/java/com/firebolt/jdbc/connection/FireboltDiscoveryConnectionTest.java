package com.firebolt.jdbc.connection;

import com.firebolt.jdbc.client.HttpClientConfig;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FireboltDiscoveryConnectionTest {

    private MockWebServer server;

    @BeforeEach
    void setUp() throws Exception {
        resetHttpClient();
        server = new MockWebServer();
        server.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        server.close();
        resetHttpClient();
    }

    @Test
    void shouldConnectUsingNoAuthDiscoveryDocumentAndQueryParameters() throws Exception {
        String queryEndpoint = server.url("/").toString();
        server.enqueue(new MockResponse()
                .setHeader("Content-Type", "application/json")
                .setBody("{\"auth\":{\"type\":\"none\"},\"endpoints\":{\"query\":\"" + queryEndpoint + "\"},\"parameters\":{\"account_id\":\"acc-1\"}}"));
        server.enqueue(new MockResponse().setBody("1\nint\n1\n"));

        String jdbcUrl = String.format("jdbc:firebolt://localhost:%s?ssl_mode=disable&database=db1&engine=eng1&query_label=from_url&compress=false",
                server.getPort());
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT 1")) {
            assertTrue(resultSet.next());
            assertEquals(1, resultSet.getInt(1));
            assertFalse(resultSet.next());
        }

        RecordedRequest discoveryRequest = server.takeRequest();
        assertEquals("/.well-known/firebolt", discoveryRequest.getPath());
        assertNull(discoveryRequest.getHeader("Authorization"));

        RecordedRequest queryRequest = server.takeRequest();
        String queryPath = queryRequest.getPath();
        assertTrue(queryPath.contains("database=db1"));
        assertTrue(queryPath.contains("engine=eng1"));
        assertTrue(queryPath.contains("account_id=acc-1"));
        assertTrue(queryPath.contains("query_label="));
        assertTrue(queryPath.contains("compress=0"));
        assertNull(queryRequest.getHeader("Authorization"));
    }

    private void resetHttpClient() throws Exception {
        Field instance = HttpClientConfig.class.getDeclaredField("instance");
        instance.setAccessible(true);
        instance.set(null, null);
    }
}
