package io.firebolt.jdbc.connection.settings;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class FireboltPropertiesTest {

  @Test
  void shouldHaveDefaultPropertiesWhenOnlyTheRequiredFieldsAreSpecified() {
    FireboltProperties expectedDefaultProperties =
        FireboltProperties.builder()
            .bufferSize(65536)
            .sslRootCertificate("")
            .sslMode("strict")
            .usePathAsDb(true)
            .path("/")
            .port(443)
            .compress(1)
            .useConnectionPool(0)
            .user(null)
            .password(null)
            .host("host")
            .ssl(true)
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
            .additionalProperties(new ArrayList<>())
            .build();

    Properties properties = new Properties();
    properties.put("host", "host");
    assertEquals(expectedDefaultProperties, FireboltProperties.of(properties));
  }

  @Test
  void shouldHaveAllTheSpecifiedCustomProperties() {
    Properties properties = new Properties();
    properties.put("buffer_size", "51");
    properties.put("socket_timeout", "20");
    properties.put("ssl", "true");
    properties.put("port", "13");
    properties.put("host", "myDummyHost");
    properties.put("database", "myDb");
    properties.put("sslRootCert", "root_cert");
    properties.put("sslmode", "none");
    properties.put("use_path_as_db", "false");
    properties.put("path", "/example");
    properties.put("check_for_redirects", "true");
    properties.put("someCustomProperties", "custom_value");
    properties.put("max_retries", "3");
    properties.put("compress", "0");

    FireboltProperties expectedDefaultProperties =
        FireboltProperties.builder()
            .bufferSize(51)
            .sslRootCertificate("root_cert")
            .maxRetries(3)
            .sslMode("none")
            .usePathAsDb(false)
            .path("/example")
            .port(13)
            .database("myDb")
            .compress(0)
            .useConnectionPool(0)
            .user(null)
            .password(null)
            .host("myDummyHost")
            .ssl(true)
            .account(null)
            .engine(null)
            .defaultMaxPerRoute(500)
            .timeToLiveMillis(60000)
            .validateAfterInactivityMillis(3000)
            .maxTotal(10000)
            .outputFormat(null)
            .socketTimeout(20)
            .connectionTimeout(2147483647)
            .keepAliveTimeout(2147483647)
            .apacheBufferSize(65536)
            .additionalProperties(
                Collections.singletonList(
                    new ImmutablePair<>("someCustomProperties", "custom_value")))
            .build();
    assertEquals(expectedDefaultProperties, FireboltProperties.of(properties));
  }

  @Test
  void shouldUsePathAsDb() {
    Properties properties = new Properties();
    properties.put("usePathAsDb", "true");
    properties.put("path", "/example");
    properties.put("host", "host");

    assertEquals("example", FireboltProperties.of(properties).getDatabase());
  }

  @Test
  void dbShouldBeNullWhenNoneProvided() {
    Properties properties = new Properties();
    properties.put("usePathAsDb", "false");
    properties.put("host", "host");

    assertNull(FireboltProperties.of(properties).getDatabase());
  }

  @Test
  void shouldThrowExceptionWhenHostIsNotProvided() {
    Properties properties = new Properties();
    assertThrows(IllegalArgumentException.class, () -> FireboltProperties.of(properties));
  }

  @Test
  void shouldThrowExceptionWhenDbPathFormatIsInvalid() {
    Properties properties = new Properties();
    properties.put("usePathAsDb", "true");
    properties.put("path", "INVALID_FORMAT");
    properties.put("host", "host");

    assertThrows(IllegalArgumentException.class, () -> FireboltProperties.of(properties));
  }
}
