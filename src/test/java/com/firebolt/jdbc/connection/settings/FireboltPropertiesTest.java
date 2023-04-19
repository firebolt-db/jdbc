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
				.sslCertificatePath("").sslMode("strict").path("").port(443) // 443 by default as SSL is enabled by
				.systemEngine(false).compress(true)													// default
				.user(null).password(null).host("host").ssl(true).additionalProperties(new HashMap<>())
				.account(null).keepAliveTimeoutMillis(300000).maxConnectionsTotal(300).maxRetries(3)
				.socketTimeoutMillis(0).connectionTimeoutMillis(60000).tcpKeepInterval(30).environment("app").tcpKeepIdle(60)
				.tcpKeepCount(10).account("firebolt").build();

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
		properties.put("path", "example");
		properties.put("someCustomProperties", "custom_value");
		properties.put("compress", "1");

		Map<String, String> customProperties = new HashMap<>();
		customProperties.put("someCustomProperties", "custom_value");

		FireboltProperties expectedDefaultProperties = FireboltProperties.builder().bufferSize(51)
				.sslCertificatePath("root_cert").sslMode("none").path("example").database("myDb").compress(true)
				.port(443).user(null).password(null).host("myDummyHost").ssl(true).systemEngine(false)
				.additionalProperties(customProperties).account(null).keepAliveTimeoutMillis(300000)
				.maxConnectionsTotal(300).maxRetries(3).socketTimeoutMillis(20).connectionTimeoutMillis(60000)
				.tcpKeepInterval(30).tcpKeepIdle(60).tcpKeepCount(10).environment("app").account("firebolt").build();
		assertEquals(expectedDefaultProperties, FireboltProperties.of(properties));
	}

	@Test
	void shouldUsePathParamAsDb() {
		Properties properties = new Properties();
		properties.put("path", "example");
		properties.put("host", "host");

		assertEquals("example", FireboltProperties.of(properties).getDatabase());
	}

	@Test
	void shouldSupportBooleansForBooleanProperties() {
		Properties properties = new Properties();
		properties.put("path", "example");
		properties.put("host", "host");
		properties.put("ssl", "true");
		properties.put("compress", "false");

		assertTrue(FireboltProperties.of(properties).isSsl());
		assertFalse(FireboltProperties.of(properties).isCompress());
	}

	@Test
	void shouldSupportIntForBooleanProperties() {
		Properties properties = new Properties();
		properties.put("path", "example");
		properties.put("host", "host");
		properties.put("ssl", "2");
		properties.put("compress", "0");

		assertTrue(FireboltProperties.of(properties).isSsl());
		assertFalse(FireboltProperties.of(properties).isCompress());
	}

	@Test
	void shouldUseCustomPortWhenProvided() {
		Properties properties = new Properties();
		properties.put("path", "example");
		properties.put("host", "host");
		properties.put("port", "999");

		assertEquals(999, FireboltProperties.of(properties).getPort());
	}

	@Test
	void shouldUseSystemEngineWhenNoDbOrEngineProvided() {
		Properties properties = new Properties();
		FireboltProperties fireboltProperties = FireboltProperties.of(properties);
		assertTrue(FireboltProperties.of(properties).isSystemEngine());
		assertEquals("system", fireboltProperties.getEngine());
		assertNull(fireboltProperties.getDatabase());
		assertFalse(fireboltProperties.isCompress());
	}

	@Test
	void shouldNotUseSystemEngineWhenDbAsPathIsProvided() {
		Properties properties = new Properties();
		properties.put("path", "example");
		FireboltProperties fireboltProperties = FireboltProperties.of(properties);
		assertFalse(FireboltProperties.of(properties).isSystemEngine());
		assertNull(fireboltProperties.getEngine());
		assertEquals("example", fireboltProperties.getDatabase());
		assertTrue(fireboltProperties.isCompress());
	}

	@Test
	void shouldNotUseSystemEngineWhenDbAsQueryParamIsProvided() {
		Properties properties = new Properties();
		properties.put("database", "example");
		FireboltProperties fireboltProperties = FireboltProperties.of(properties);
		assertFalse(FireboltProperties.of(properties).isSystemEngine());
		assertNull(fireboltProperties.getEngine());
		assertEquals("example", fireboltProperties.getDatabase());
		assertTrue(fireboltProperties.isCompress());
	}

	@Test
	void shouldNotUseSystemEngineWhenEngineIsProvided() {
		Properties properties = new Properties();
		properties.put("engine", "example");
		FireboltProperties fireboltProperties = FireboltProperties.of(properties);
		assertFalse(FireboltProperties.of(properties).isSystemEngine());
		assertNull(fireboltProperties.getDatabase());
		assertEquals("example", fireboltProperties.getEngine());
		assertTrue(fireboltProperties.isCompress());
	}

	@Test
	void shouldSupportUserClientsAndDrivers() {
		String clients = "ConnA:1.0.9,ConnB:2.8.0";
		String drivers = "DriverA:2.0.9,DriverB:3.8.0";
		Properties properties = new Properties();
		properties.put("user_clients", clients);
		properties.put("user_drivers", drivers);
		properties.put("host", "host");
		properties.put("database", "db");

		assertEquals(clients, FireboltProperties.of(properties).getUserClients());
		assertEquals(drivers, FireboltProperties.of(properties).getUserDrivers());
	}

}
