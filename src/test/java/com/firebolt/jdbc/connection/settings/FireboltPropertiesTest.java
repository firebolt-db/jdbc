package com.firebolt.jdbc.connection.settings;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FireboltPropertiesTest {

	@Test
	void shouldHaveDefaultPropertiesWhenOnlyTheRequiredFieldsAreSpecified() {
		FireboltProperties expectedDefaultProperties = FireboltProperties.builder().engine("engine").database("db").bufferSize(65536)
				.sslCertificatePath("").sslMode("strict").path("").port(443) // 443 by default as SSL is enabled by
				.systemEngine(false).compress(true)													// default
				.principal(null).secret(null).host("host").ssl(true).additionalProperties(new HashMap<>())
				.keepAliveTimeoutMillis(300000).maxConnectionsTotal(300).maxRetries(3)
				.socketTimeoutMillis(0).connectionTimeoutMillis(60000).tcpKeepInterval(30).environment("app").tcpKeepIdle(60)
				.tcpKeepCount(10).build();

		Properties properties = new Properties();
		properties.put("engine", "engine");
		properties.put("host", "host");
		properties.put("database", "db");
		assertEquals(expectedDefaultProperties, FireboltProperties.of(properties));
	}

	@Test
	void shouldHaveAllTheSpecifiedCustomProperties() {
		Properties properties = new Properties();
		properties.put("engine", "my_test");
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

		FireboltProperties expectedDefaultProperties = FireboltProperties.builder().engine("my_test").bufferSize(51)
				.sslCertificatePath("root_cert").sslMode("none").path("example").database("myDb").compress(true)
				.port(443).principal(null).secret(null).host("myDummyHost").ssl(true).systemEngine(false)
				.additionalProperties(customProperties).keepAliveTimeoutMillis(300000)
				.maxConnectionsTotal(300).maxRetries(3).socketTimeoutMillis(20).connectionTimeoutMillis(60000)
				.tcpKeepInterval(30).tcpKeepIdle(60).tcpKeepCount(10).environment("app").build();
		assertEquals(expectedDefaultProperties, FireboltProperties.of(properties));
	}

	@Test
	void shouldAddAdditionalProperties() {
		FireboltProperties props = FireboltProperties.of();
		assertTrue(props.getAdditionalProperties().isEmpty());
		props.addProperty(new ImmutablePair<>("a", "1"));
		props.addProperty("b", "2");
		assertEquals(Map.of("a", "1", "b", "2"), props.getAdditionalProperties());
	}

	@Test
	void shouldUsePathParamAsDb() {
		Properties properties = new Properties();
		properties.put("path", "example");
		properties.put("host", "host");

		assertEquals("example", FireboltProperties.of(properties).getDatabase());
	}

	@ParameterizedTest
	@ValueSource(strings = {"$", "@"})
	void invalidDatabase(String db) {
		Properties properties = new Properties();
		properties.put("path", db);
		assertThrows(IllegalArgumentException.class, () -> FireboltProperties.of(properties));
	}

	@Test
	void emptyCopy() {
		Properties properties = new Properties();
		properties.put("path", "example");
		properties.put("host", "host");
		assertEquals(FireboltProperties.of(properties), FireboltProperties.copy(FireboltProperties.of(properties)));
	}

	@Test
	void notEmptyCopy() {
		assertEquals(FireboltProperties.of(), FireboltProperties.copy(FireboltProperties.of()));
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
		assertTrue(fireboltProperties.isSystemEngine());
		assertNull(fireboltProperties.getEngine());
		assertNull(fireboltProperties.getDatabase());
		assertFalse(fireboltProperties.isCompress());
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

	@Test
	void noEngineNoDbSystemEngine() {
		assertNull(FireboltProperties.of(new Properties()).getEngine());
	}

	@ParameterizedTest
	@CsvSource(value = {
			"env, qa,,api.qa.firebolt.io,qa",
			"environment, test,,api.test.firebolt.io,test",
			"env, staging,super-host.com,super-host.com,staging",
			"env,,my-host.com,my-host.com,app",
			"env,,api.dev.firebolt.io,api.dev.firebolt.io,dev",
			"env,,something.io,something.io,app", // not standard host, no configured environment -> default environment
			",,,api.app.firebolt.io,app", // no host, no environment -> default environment (app) and default host api.app.firebolt.io
			",,api.app.firebolt.io,api.app.firebolt.io,app", // no configured environment, discover default environment from host
			",,api.dev.firebolt.io,api.dev.firebolt.io,dev", // no configured environment, discover not default environment from host
	}, delimiter = ',')
	void hostAndEnvironment(String envKey, String envValue, String host, String expectedHost, String expectedEnvironment) {
		Properties properties = properties(envKey, envValue, host);
		assertEquals(expectedHost, FireboltProperties.of(properties).getHost());
		assertEquals(expectedEnvironment, FireboltProperties.of(properties).getEnvironment());
	}

	@ParameterizedTest
	@CsvSource(value = {
			"env,app,api.dev.firebolt.io",
			"env,qa,api.app.firebolt.io",
	}, delimiter = ',')
	void environmentDoesNotMatch(String envKey, String envValue, String host) {
		Properties properties = properties(envKey, envValue, host);
		assertThrows(IllegalStateException.class, () -> FireboltProperties.of(properties));
	}

	private Properties properties(String envKey, String envValue, String host) {
		Properties properties = new Properties();
		if (envValue != null) {
			properties.put(envKey, envValue);
		}
		if (host != null) {
			properties.put("host", host);
		}
		return properties;
	}

}
