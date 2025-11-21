package com.firebolt.jdbc.connection;

import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.statement.preparedstatement.FireboltParquetStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FireboltCoreConnectionTest {

    private static final String VALID_URL_WITH_DB = "jdbc:firebolt:my_db?";
    private static final String VALID_URL_WITHOUT_DB = "jdbc:firebolt:?";

    @Mock
    private Statement mockStatement;

    @ParameterizedTest(name = "Valid URL: {0}")
    @ValueSource(strings = {
            "http://localhost:8080",
            "https://example.com:443",
            "http://192.168.1.1:8080",
            "https://[2001:db8::1]:8080",
            "http://sub.example.com:8080",
            "http://cluster.local:8080",
            "https://cluster.local:8080",
            "http://valid.hostname.:8080",
            "http://valid..hostname:8080",
            "http://.valid.hostname:8080",
            "http://host_name:8080",
            "http://-hostname:8080",
            "http://hostname-:8080",
            "http://256.256.256.256:8080",
            "http://270.0.0.1:8080",
            "http://0.270.0.1:8080",
            "http://0.0.270.1:8080",
            "http://0.0.0.270:8080",
            "http://127.0.1:8080",
            "http://127.0.0:8080",
            "http://127.0:8080",
            "http://127:8080",
            "http://127.0.0.1.1:8080",
            "http://127.0.0.0.0.1:8080",
            "http://127.0.0.:8080",
            "http://127..0.1:8080",
            "http://127.0.0.01:8080",
            "http://127.abc.0.1:8080",
            "http://127.0.0.0x1:8080",
            "http://300.300.300.300:8080",
            "http://127.0.0.-1:8080",
            "http://127.0.0.+1:8080",

            // Kubernetes-style hostnames
            "http://firebolt-core-svc:8080",
            "http://firebolt-core-svc.namespace-foo:8080",
            "http://firebolt-core-svc.namespace-foo.svc:8080",
            "http://firebolt-core-svc.namespace-foo.svc.cluster:8080",
            "http://firebolt-core-svc.namespace-foo.svc.cluster.local:8080"

    })
    void testValidUrls(String url) throws SQLException {
        when(mockStatement.executeUpdate("USE DATABASE \"my_db\"")).thenReturn(0);
        assertDoesNotThrow(() -> createConnection(url));
    }

    @ParameterizedTest(name = "Invalid URL: {0}")
    @ValueSource(strings = {
            "mydomain.com:8080",
            "http://",
            "https://",
            "http://localhost",
            "http://:8080",
            "http://localhost:",
            "http://localhost:0",
            "http://localhost:70000",
            "http://localhost:-1",
            "invalid://localhost:8080",
            // Hostname longer than 256 characters (should fail) - this is 300+ chars
            "http://very-long-hostname-that-exceeds-the-maximum-dns-limit-abcdefghijklmnopqrstuvwxyz-abcdefghijklmnopqrstuvwxyz-abcdefghijklmnopqrstuvwxyz-abcdefghijklmnopqrstuvwxyz-abcdefghijklmnopqrstuvwxyz-abcdefghijklmnopqrstuvwxyz-abcdefghijklmnopqrstuvwxyz-abcdefghijklmnopqrstuvwxyz-abcdefghijklmnopqrstuvwxyz-extra:8080",
            // Hostname with one label longer than 63 characters (should fail)
            "http://short.this-label-is-way-too-long-and-exceeds-the-maximum-allowed-length-of-63-characters-for-a-single-dns-label.com:8080"
    })
    void testInvalidUrls(String url) {
        SQLException exception = assertThrows(SQLException.class, () -> createConnection(url));
        assertTrue(exception.getMessage().contains("Invalid URL format") ||
                exception.getMessage().contains("not valid"));
    }

    @Test
    void testMissingUrl() {
        SQLException exception = assertThrows(SQLException.class, () -> createConnection(null));
        assertTrue(exception.getMessage().contains("Url is required for firebolt core"));
    }

    @Test
    void canConnectToCoreWithoutSpecifyingADb() throws SQLException {
        StringBuilder jdbcUrlBuilder = new StringBuilder(VALID_URL_WITHOUT_DB);
        jdbcUrlBuilder.append("&url=").append("http://localhost:3473");

        try (FireboltCoreConnection connection = new FireboltCoreConnection(jdbcUrlBuilder.toString(), new Properties())) {
            FireboltProperties fireboltProperties = connection.getSessionProperties();
            assertFalse(fireboltProperties.isSsl());
            assertEquals("localhost", fireboltProperties.getHost());
            assertEquals(3473, fireboltProperties.getPort());
            assertTrue(StringUtils.isBlank(fireboltProperties.getDatabase()));
        }

    }

    @Test
    void canConnectOverHttp() throws SQLException {
        Map<String, String> connectionParams = Map.of(
                "url", "http://localhost:3473",
                "database", "my_db"
        );
        try (FireboltCoreConnection connection = createConnectionWithParams(connectionParams)) {
            FireboltProperties fireboltProperties = connection.getSessionProperties();
            assertFalse(fireboltProperties.isSsl());
            assertEquals("localhost", fireboltProperties.getHost());
            assertEquals(3473, fireboltProperties.getPort());
            assertEquals("my_db", fireboltProperties.getDatabase());
        }
    }

    @Test
    void canConnectOverHttps() throws SQLException {
        when(mockStatement.executeUpdate("USE DATABASE \"my_db\"")).thenReturn(0);

        Map<String, String> connectionParams = Map.of(
                "url", "https://localhost:3473",
                "database", "my_db"
        );

        try (FireboltCoreConnection connection = createConnectionWithParams(connectionParams)) {
            FireboltProperties fireboltProperties = connection.getSessionProperties();
            assertTrue(fireboltProperties.isSsl());
            assertEquals("localhost", fireboltProperties.getHost());
            assertEquals(3473, fireboltProperties.getPort());
            assertEquals("my_db", fireboltProperties.getDatabase());
        }
    }

    @Test
    void createParquetStatement() throws SQLException {
        Map<String, String> connectionParams = Map.of(
                "url", "http://localhost:3473",
                "database", "my_db"
        );
        try (FireboltCoreConnection connection = createConnectionWithParams(connectionParams)) {
            FireboltParquetStatement parquetStatement = connection.createParquetStatement();
            assertNotNull(parquetStatement);
            assertFalse(parquetStatement.isClosed());
        }
    }

    private FireboltCoreConnection createConnection(String url) throws SQLException {
        StringBuilder jdbcUrlBuilder = new StringBuilder(VALID_URL_WITH_DB);

        if (url != null) {
            jdbcUrlBuilder.append("&url=").append(url);
        }

        return aFireboltCoreConnection(jdbcUrlBuilder.toString(), new Properties());
    }

    private FireboltCoreConnection createConnectionWithParams(Map<String, String> parameters) throws SQLException {
        StringBuilder jdbcUrlBuilder = new StringBuilder(VALID_URL_WITH_DB);

        String params = parameters.entrySet().stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining("&"));

        jdbcUrlBuilder.append("&").append(params);
        return aFireboltCoreConnection(jdbcUrlBuilder.toString(), new Properties());
    }

    private FireboltCoreConnection aFireboltCoreConnection(String jdbcUrl, Properties properties) throws SQLException {
        return new FireboltCoreConnection(jdbcUrl, properties){
            @Override
            public Statement createStatement() {
                return mockStatement;
            }
        };
    }
}
