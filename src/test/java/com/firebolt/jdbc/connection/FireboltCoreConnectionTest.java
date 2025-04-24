package com.firebolt.jdbc.connection;

import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FireboltCoreConnectionTest {

    private static final String VALID_URL = "jdbc:firebolt:my_db?";
    private static final int VALID_PORT = 8080;

    @ParameterizedTest(name = "Valid IPv4 address: {0}")
    @ValueSource(strings = {
        "192.168.1.1",
        "10.0.0.0",
        "172.16.254.1",
        "192.0.2.1",
        "127.0.0.1",
        "0.0.0.0",
        "255.255.255.255"
    })
    void testValidIPv4Addresses(String address) {
        assertDoesNotThrow(() -> createConnection(address, VALID_PORT));
    }

    @ParameterizedTest(name = "Invalid IPv4 address: {0}")
    @ValueSource(strings = {
        "256.1.2.3",
        "1.2.3.256",
        "192.168.1",
        "192.168.1.1.1",
        "192.168.1.",
        ".192.168.1.1",
        "192.168.1.a",
        "192.168.001.1"
    })
    void testInvalidIPv4Addresses(String address) {
        SQLException exception = assertThrows(SQLException.class, 
            () -> createConnection(address, VALID_PORT));
        assertTrue(exception.getMessage().contains("Invalid host"));
    }

    @ParameterizedTest(name = "Valid IPv6 address: {0}")
    @ValueSource(strings = {
        "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
        "2001:db8:85a3:0:0:8a2e:370:7334",
        "2001:db8:85a3::8a2e:370:7334",
        "::1",
        "::",
        "fe80::1",
        "fe80::217:f2ff:fe07:ed62"
    })
    void testValidIPv6Addresses(String address) {
        assertDoesNotThrow(() -> createConnection(address, VALID_PORT));
    }

    @ParameterizedTest(name = "Invalid IPv6 address: {0}")
    @ValueSource(strings = {
        "2001:0db8:85a3:0000:0000:8a2e:0370:7334:7334",
        "2001:db8:85a3:0:0:8a2e:370g:7334",
        "2001:db8:85a3::8a2e::370:7334",
        ":::1",
        "fe80:",
        "fe80::217::f2ff:fe07:ed62"
    })
    void testInvalidIPv6Addresses(String address) {
        SQLException exception = assertThrows(SQLException.class, 
            () -> createConnection(address, VALID_PORT));
        assertTrue(exception.getMessage().contains("Invalid host"));
    }

    @ParameterizedTest(name = "Valid hostname: {0}")
    @ValueSource(strings = {
        "localhost",
        "example.com",
        "sub.example.com",
        "sub-domain.example.com",
        "host.mydomain.com",
        "a.com"
    })
    void testValidHostnames(String hostname) {
        assertDoesNotThrow(() -> createConnection(hostname, VALID_PORT));
    }

    @ParameterizedTest(name = "Invalid hostname: {0}")
    @ValueSource(strings = {
        "my-server",
        "invalid..hostname",
        ".invalid.hostname",
        "invalid.hostname.",
        "invalid@hostname",
        "host_name",
        "-hostname",
        "hostname-",
        "host@name.com",
        "host#name.com",
        "host$name.com",
        "host%25name.com",  // "host%name"
        "host%5Ename.com",  // "host^name"
        "host&name.com",
        "host*name.com",
        "host(name.com",
        "host)name.com",
        "host+name.com",
        "host%7Bname.com",     // "host{name.com"
        "host%7Dname.com",    //  "host}name.com"
        "host[name.com",
        "host]name.com",
        "host%7Cname.com",     //  "host|name.com"
        "host%5C%5Cname.com",  //  "host\\name.com"
        "host/name.com",
        "host?name.com",
        "host%3Cname.com",       // "host<name.com"
        "host%3Ename.com",       // "host>name.com"
        "host,name.com",
        "host;name.com",
        "host:name.com",
        "host%5C%22name.com",       // "host\"name.com"
        "host'name.com"
    })
    void testInvalidHostnames(String hostname) {
        SQLException exception = assertThrows(SQLException.class,
            () -> createConnection(hostname, VALID_PORT));
        assertTrue(exception.getMessage().contains("Invalid host"));
    }

    @ParameterizedTest(name = "Valid port: {0}")
    @ValueSource(ints = {1, 80, 443, 8080, 3306, 65535})
    void testValidPorts(int port) {
        assertDoesNotThrow(() -> createConnection("localhost", port));
    }

    @ParameterizedTest(name = "Invalid port: {0}")
    @ValueSource(ints = {0, -1, 65536, 70000})
    void testInvalidPorts(int port) {
        SQLException exception = assertThrows(SQLException.class, 
            () -> createConnection("localhost", port));
        assertTrue(exception.getMessage().contains("Port must be a positive number between 1 and 65535"));
    }

    @Test
    @Disabled // when host is empty it will default to api.app.firebolt.io
    void testMissingHost() {
        SQLException exception = assertThrows(SQLException.class, 
            () -> createConnection(null, VALID_PORT));
        assertTrue(exception.getMessage().contains("Host and port are required"));
    }

    @Test
    @Disabled // need to check if it is ok to default to 443 for ssl and 9090 for non-ssl
    void testMissingPort() {
        SQLException exception = assertThrows(SQLException.class, 
            () -> createConnection("localhost", null));
        assertTrue(exception.getMessage().contains("Host and port are required"));
    }

    private FireboltCoreConnection createConnection(String host, Integer port) throws SQLException {
        StringBuilder jdbcUrlBuilder = new StringBuilder(VALID_URL)
            .append("connection_type=core");

        if (host != null) {
            jdbcUrlBuilder.append("&host=").append(host);
        }

        if (port != null) {
            jdbcUrlBuilder.append("&port=").append(port);
        }

        return new FireboltCoreConnection(jdbcUrlBuilder.toString(), new Properties());
    }
} 