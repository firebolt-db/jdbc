package io.firebolt.jdbc.connection.settings;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PropertiesToFireboltPropertiesTransformerTest {

    private final PropertiesToFireboltPropertiesTransformer transformer = new PropertiesToFireboltPropertiesTransformer();

    @Test
    void shouldHaveDefaultPropertiesWhenOnlyTheRequiredFieldsAreSpecified() {
        FireboltProperties expectedDefaultProperties = FireboltProperties.builder()
                .bufferSize(65536)
                .sslRootCertificate("")
                .sslMode("strict")
                .usePathAsDb(true)
                .path("/")
                .port(443) // 443 by default as SSL is enabled by default
                .database("default") // detabase is "default" by default when the path is "/"
                .compress(1)
                .useConnectionPool(0)
                .user(null)
                .password(null)
                .host("https://host") // host is appended with https:// because SSL is enabled
                .ssl(true)
                .customProperties(new Properties())
                .account(null)
                .engine(null)
                .defaultMaxPerRoute(500)
                .timeToLiveMillis(60000)
                .validateAfterInactivityMillis(3000)
                .maxTotal(10000)
                .maxRetries(3)
                .outputFormat(null)
                .socketTimeout(2147483647)
                .connectionTimeout(2147483647)
                .keepAliveTimeout(2147483647)
                .apacheBufferSize(65536)
                .build();

        Properties properties = new Properties();
        properties.put("host", "host"); // host is mandatory
        assertEquals(expectedDefaultProperties, new PropertiesToFireboltPropertiesTransformer().apply(properties));
    }

    @Test
    void shouldHaveAllTheSpecifiedCustomProperties() {
        Properties properties = new Properties();
        properties.put("bufferSize", "51");
        properties.put("socketTimeout", "20");
        properties.put("ssl", "true");
        properties.put("port", "13");
        properties.put("host", "myDummyHost");
        properties.put("database", "myDb");
        properties.put("sslRootCert", "root_cert");
        properties.put("sslMode", "none");
        properties.put("usePathAsDb", "false");
        properties.put("path", "/example");
        properties.put("checkForRedirects", "true");
        properties.put("someCustomProperties", "custom_value");
        properties.put("compress", "0");


        Properties customProperties = new Properties();
        customProperties.put("someCustomProperties", "custom_value");

        FireboltProperties expectedDefaultProperties = FireboltProperties.builder()
                .bufferSize(51)
                .sslRootCertificate("root_cert")
                .sslMode("none")
                .usePathAsDb(false)
                .path("/example")
                .port(13)
                .database("myDb")
                .compress(0)
                .useConnectionPool(0)
                .user(null)
                .password(null)
                .host("https://myDummyHost")
                .ssl(true)
                .customProperties(customProperties)
                .account(null)
                .engine(null)
                .defaultMaxPerRoute(500)
                .timeToLiveMillis(60000)
                .validateAfterInactivityMillis(3000)
                .maxTotal(10000)
                .maxRetries(3)
                .outputFormat(null)
                .socketTimeout(20)
                .connectionTimeout(2147483647)
                .keepAliveTimeout(2147483647)
                .apacheBufferSize(65536)
                .build();
        assertEquals(expectedDefaultProperties, transformer.apply(properties));
    }

    @Test
    void shouldUsePathAsDb() {
        Properties properties = new Properties();
        properties.put("usePathAsDb", "true");
        properties.put("path", "/example");
        properties.put("host", "host");

        assertEquals("example", transformer.apply(properties).getDatabase());
    }

    @Test
    void shouldUseDefaultDbWhenDatabaseNameIsEmpty() {
        Properties properties = new Properties();
        properties.put("usePathAsDb", "false");
        properties.put("host", "host");

        assertEquals("default", transformer.apply(properties).getDatabase());
    }

    @Test
    void shouldThrowExceptionWhenHostIsNotProvided() {
        Properties properties = new Properties();
        assertThrows(IllegalArgumentException.class, () -> transformer.apply(properties));
    }

    @Test
    void shouldThrowExceptionWhenDbPathFormatIsInvalid() {
        Properties properties = new Properties();
        properties.put("usePathAsDb", "true");
        properties.put("path", "");
        properties.put("host", "host");

        assertThrows(IllegalArgumentException.class, () -> transformer.apply(properties));
    }

}