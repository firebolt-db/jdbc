package com.firebolt.jdbc.connection.settings;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.Test;

class FireboltPropertiesTest {

	@Test
	void shouldHaveDefaultPropertiesWhenOnlyTheRequiredFieldsAreSpecified() {
		FireboltProperties expectedDefaultProperties = FireboltProperties.builder().database("db").bufferSize(65536)
				.sslCertificatePath("").sslMode("strict").path("/").port(443) // 443 by default as SSL is enabled by
																				// default
				.compress(true).user(null).password(null).host("host").ssl(true).additionalProperties(new HashMap<>())
				.account(null).engine(null).maxConnectionsPerRoute(500).timeToLiveMillis(60000)
				.validateAfterInactivityMillis(3000).maxConnectionsTotal(10000).maxRetries(3).socketTimeoutMillis(0)
				.connectionTimeoutMillis(0).keepAliveTimeoutMillis(Integer.MAX_VALUE).clientBufferSize(65536)
				.tcpKeepInterval(30).tcpKeepIdle(60).tcpKeepCount(10).build();

		Properties properties = new Properties();
		properties.put("host", "host");
		properties.put("database", "db");
		assertEquals(expectedDefaultProperties, FireboltProperties.of(properties));
	}

	@Test
	void shouldHaveAllTheSpecifiedCustomProperties() {
		Properties properties = new Properties();
		properties.put("buffer_size", "51");
		properties.put("socket_timeout_millis", "20");
		properties.put("ssl", "1");
		properties.put("host", "myDummyHost");
		properties.put("database", "myDb");
		properties.put("ssl_certificate_path", "root_cert");
		properties.put("ssl_mode", "none");
		properties.put("path", "/example");
		properties.put("someCustomProperties", "custom_value");
		properties.put("compress", "1");

		Map<String, String> customProperties = new HashMap<>();
		customProperties.put("someCustomProperties", "custom_value");

		FireboltProperties expectedDefaultProperties = FireboltProperties.builder().bufferSize(51)
				.sslCertificatePath("root_cert").sslMode("none").path("/example").database("myDb").compress(true)
				.port(443).user(null).password(null).host("myDummyHost").ssl(true)
				.additionalProperties(customProperties).account(null).engine(null).maxConnectionsPerRoute(500)
				.timeToLiveMillis(60000).validateAfterInactivityMillis(3000).maxConnectionsTotal(10000).maxRetries(3)
				.socketTimeoutMillis(20).connectionTimeoutMillis(0).keepAliveTimeoutMillis(Integer.MAX_VALUE)
				.clientBufferSize(65536).tcpKeepInterval(30).tcpKeepIdle(60).tcpKeepCount(10).build();
		assertEquals(expectedDefaultProperties, FireboltProperties.of(properties));
	}

	@Test
	void shouldUsePathParamAsDb() {
		Properties properties = new Properties();
		properties.put("path", "/example");
		properties.put("host", "host");

		assertEquals("example", FireboltProperties.of(properties).getDatabase());
	}

	@Test
	void shouldSupportBooleansForBooleanProperties() {
		Properties properties = new Properties();
		properties.put("path", "/example");
		properties.put("host", "host");
		properties.put("ssl", "true");
		properties.put("compress", "false");

		assertTrue(FireboltProperties.of(properties).isSsl());
		assertFalse(FireboltProperties.of(properties).isCompress());
	}

	@Test
	void shouldSupportIntForBooleanProperties() {
		Properties properties = new Properties();
		properties.put("path", "/example");
		properties.put("host", "host");
		properties.put("ssl", "2");
		properties.put("compress", "0");

		assertTrue(FireboltProperties.of(properties).isSsl());
		assertFalse(FireboltProperties.of(properties).isCompress());
	}

	@Test
	void shouldUseCustomPortWhenProvided() {
		Properties properties = new Properties();
		properties.put("path", "/example");
		properties.put("host", "host");
		properties.put("port", "999");

		assertEquals(999, FireboltProperties.of(properties).getPort());
	}

	@Test
	void shouldThrowExceptionWhenNoDbProvided() {
		Properties properties = new Properties();
		properties.put("host", "host");

		assertThrows(IllegalArgumentException.class, () -> FireboltProperties.of(properties));
	}

	@Test
	void shouldThrowExceptionWhenHostIsNotProvided() {
		Properties properties = new Properties();
		assertThrows(IllegalArgumentException.class, () -> FireboltProperties.of(properties));
	}

	@Test
	void shouldThrowExceptionWhenDbPathFormatIsInvalid() {
		Properties properties = new Properties();
		properties.put("path", "");
		properties.put("host", "host");

		assertThrows(IllegalArgumentException.class, () -> FireboltProperties.of(properties));
	}

	@Test
	void shouldHaveAggressiveCancelBeDisabledByDefault() {
		assertFalse(FireboltProperties.builder().build().isAggressiveCancel());
	}

	@Test
	void shouldNotReturnAliasAsCustomProperty() {
		Properties properties = new Properties();
		properties.put("path", "");
		properties.put("host", "host");

		assertThrows(IllegalArgumentException.class, () -> FireboltProperties.of(properties));
	}
}
