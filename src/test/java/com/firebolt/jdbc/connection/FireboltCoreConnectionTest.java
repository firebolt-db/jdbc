package com.firebolt.jdbc.connection;

import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FireboltCoreConnectionTest {

    private static final String VALID_URL = "jdbc:firebolt:my_db?";

    @ParameterizedTest(name = "Valid URL: {0}")
    @ValueSource(strings = {
        "http://localhost:8080",
        "https://example.com:443",
        "http://192.168.1.1:8080",
        "https://[2001:db8::1]:8080",
        "http://sub.example.com:8080"
    })
    void testValidUrls(String url) {
        assertDoesNotThrow(() -> createConnection(url));
    }

    @ParameterizedTest(name = "Invalid URL: {0}")
    @ValueSource(strings = {
        "http://",
        "https://",
        "http://localhost",
        "http://:8080",
        "http://localhost:",
        "http://localhost:0",
        "http://localhost:70000",
        "http://localhost:-1",
        "invalid://localhost:8080",
        "http://invalid..hostname:8080",
        "http://.invalid.hostname:8080",
        "http://invalid.hostname.:8080",
        "http://invalid@hostname:8080",
        "http://host_name:8080",
        "http://-hostname:8080",
        "http://hostname-:8080",
        // Invalid IPv4 addresses
        "http://256.256.256.256:8080",  // All octets > 255
        "http://270.0.0.1:8080",        // First octet > 255
        "http://0.270.0.1:8080",        // Second octet > 255
        "http://0.0.270.1:8080",        // Third octet > 255
        "http://0.0.0.270:8080",        // Fourth octet > 255
        "http://127.0.1:8080",          // Missing octet
        "http://127.0.0:8080",          // Missing octet
        "http://127.0:8080",            // Missing two octets
        "http://127:8080",              // Missing three octets
        "http://127.0.0.1.1:8080",      // Extra octet
        "http://127.0.0.0.0.1:8080",    // Too many octets
        "http://127.0.0.:8080",         // Trailing dot
        "http://127..0.1:8080",         // Double dot
        "http://127.0.0.01:8080",       // Leading zero
        "http://127.abc.0.1:8080",      // Letters in octet
        "http://127.0.0.0x1:8080",      // Hex notation
        "http://300.300.300.300:8080",  // All octets way above 255
        "http://127.0.0.-1:8080",       // Negative number in octet
        "http://127.0.0.+1:8080"        // Plus sign in octet
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

    private FireboltCoreConnection createConnection(String url) throws SQLException {
        StringBuilder jdbcUrlBuilder = new StringBuilder(VALID_URL);

        if (url != null) {
            jdbcUrlBuilder.append("&url=").append(url);
        }

        return new FireboltCoreConnection(jdbcUrlBuilder.toString(), new Properties());
    }
} 