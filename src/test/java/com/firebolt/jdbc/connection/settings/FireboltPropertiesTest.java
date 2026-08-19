package com.firebolt.jdbc.connection.settings;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

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
				.principal(null).secret(null).host("host").ssl(true).initialAdditionalProperties(new HashMap<>())
				.keepAliveTimeoutMillis(300000).maxConnectionsTotal(300).maxRetries(3)
				.socketTimeoutMillis(0).connectionTimeoutMillis(60000).tcpKeepInterval(30).environment("app").tcpKeepIdle(60)
				.tcpKeepCount(10).connectionCachingEnabled(true).preparedStatementParamStyle("native")
				.compressRequestPayload(false)
				.fileDownloaderType("one_ahead")
				.build();

		Properties properties = new Properties();
		properties.put("engine", "engine");
		properties.put("host", "host");
		properties.put("database", "db");
		assertEquals(expectedDefaultProperties, new FireboltProperties(properties));
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
		properties.put("validate_on_system_engine", "true");
		properties.put("merge_prepared_statement_batches", "true");
		properties.put("prepared_statement_param_style", "fb_numeric");

		Map<String, String> customProperties = new HashMap<>();
		customProperties.put("someCustomProperties", "custom_value");

		FireboltProperties expectedDefaultProperties = FireboltProperties.builder().engine("my_test").bufferSize(51)
				.sslCertificatePath("root_cert").sslMode("none").path("example").database("myDb").compress(true)
				.port(443).principal(null).secret(null).host("myDummyHost").ssl(true).systemEngine(false)
				.initialAdditionalProperties(customProperties).keepAliveTimeoutMillis(300000)
				.maxConnectionsTotal(300).maxRetries(3).socketTimeoutMillis(20).connectionTimeoutMillis(60000)
				.tcpKeepInterval(30).tcpKeepIdle(60).tcpKeepCount(10).environment("app").validateOnSystemEngine(true)
				.mergePreparedStatementBatches(true).connectionCachingEnabled(true).preparedStatementParamStyle("fb_numeric")
				.compressRequestPayload(false)
				.fileDownloaderType("one_ahead")
				.build();
		assertEquals(expectedDefaultProperties, new FireboltProperties(properties));
	}

	@Test
	void shouldAddAdditionalProperties() {
		FireboltProperties props = new FireboltProperties(new Properties());
		assertTrue(props.getAdditionalProperties().isEmpty());
		props.addProperty(Map.entry("a", "1"), true);
		props.addProperty("b", "2", true);
		assertEquals(Map.of("a", "1", "b", "2"), props.getAdditionalProperties());
	}

	@Test
	void updateSpecialProperties() {
		Properties properties = new Properties();
		properties.put("database", "db1");
		properties.put("engine", "e1");
		properties.put("account_id", "a1");
		properties.put("more", "less");
		FireboltProperties props = new FireboltProperties(properties);
		assertEquals("db1", props.getDatabase());
		assertEquals("e1", props.getEngine());
		assertEquals("a1", props.getAccountId());
		assertEquals(Map.of("more", "less"), props.getAdditionalProperties());

		props.addProperty("database", "db2", true);
		props.addProperty("engine", "e2", true);
		props.addProperty("account_id", "a1", true);
		assertThrows(IllegalStateException.class, () -> props.addProperty("account_id", "a2", true));
		props.addProperty("more", "is more", true);

		assertEquals("db2", props.getDatabase());
		assertEquals("e2", props.getEngine());
		assertEquals("a1", props.getAccountId());
		assertEquals(Map.of("more", "is more"), props.getAdditionalProperties());
	}

	@Test
	void shouldUsePathParamAsDb() {
		Properties properties = new Properties();
		properties.put("path", "example");
		properties.put("host", "host");
		assertEquals("example", new FireboltProperties(properties).getDatabase());
	}

	@ParameterizedTest
	@ValueSource(strings = {"$", "@"})
	void invalidDatabase(String db) {
		Properties properties = new Properties();
		properties.put("path", db);
		assertThrows(IllegalArgumentException.class, () -> new FireboltProperties(properties));
	}

	@Test
	void emptyCopy() {
		Properties properties = new Properties();
		properties.put("path", "example");
		properties.put("host", "host");
		assertEquals(new FireboltProperties(properties), FireboltProperties.copy(new FireboltProperties(properties)));
	}

	@Test
	void notEmptyCopy() {
		assertEquals(new FireboltProperties(new Properties()), FireboltProperties.copy(new FireboltProperties(new Properties())));
	}

	@Test
	void shouldSupportBooleansForBooleanProperties() {
		Properties properties = new Properties();
		properties.put("path", "example");
		properties.put("host", "host");
		properties.put("ssl", "true");
		properties.put("compress", "false");

		assertTrue(new FireboltProperties(properties).isSsl());
		assertFalse(new FireboltProperties(properties).isCompress());
	}

	@Test
	void shouldSupportIntForBooleanProperties() {
		Properties properties = new Properties();
		properties.put("path", "example");
		properties.put("host", "host");
		properties.put("ssl", "2");
		properties.put("compress", "0");

		assertTrue(new FireboltProperties(properties).isSsl());
		assertFalse(new FireboltProperties(properties).isCompress());
	}

	@Test
	void shouldUseCustomPortWhenProvided() {
		Properties properties = new Properties();
		properties.put("path", "example");
		properties.put("host", "host");
		properties.put("port", "999");

		assertEquals(999, new FireboltProperties(properties).getPort());
	}

	@Test
	void shouldUseSystemEngineWhenNoDbOrEngineProvided() {
		Properties properties = new Properties();
		FireboltProperties fireboltProperties = new FireboltProperties(properties);
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

		assertEquals(clients, new FireboltProperties(properties).getUserClients());
		assertEquals(drivers, new FireboltProperties(properties).getUserDrivers());
	}

	@Test
	void noEngineNoDbSystemEngine() {
		assertNull(new FireboltProperties(new Properties()).getEngine());
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
		assertEquals(expectedHost, new FireboltProperties(properties).getHost());
		assertEquals(expectedEnvironment, new FireboltProperties(properties).getEnvironment());
	}

	@ParameterizedTest
	@CsvSource(value = {
			"env,app,api.dev.firebolt.io",
			"env,qa,api.app.firebolt.io",
	}, delimiter = ',')
	void environmentDoesNotMatch(String envKey, String envValue, String host) {
		Properties properties = properties(envKey, envValue, host);
		assertThrows(IllegalStateException.class, () -> new FireboltProperties(properties));
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

	@Test
	void shouldRemoveTransactionIdProperty() {
		FireboltProperties props = new FireboltProperties(new Properties());
		props.addProperty("transaction_id", "tx123", false);
		
		assertEquals("tx123", props.getTransactionId());
		props.removeProperty("transaction_id");
		assertNull(props.getTransactionId());
	}

	@Test
	void shouldRemoveTransactionSequenceIdProperty() {
		FireboltProperties props = new FireboltProperties(new Properties());
		props.addProperty("transaction_sequence_id", "seq456", false);
		
		assertEquals("seq456", props.getTransactionSequenceId());
		props.removeProperty("transaction_sequence_id");
		assertNull(props.getTransactionSequenceId());
	}

	@Test
	void shouldRemoveCustomPropertyFromRuntimeAdditionalProperties() {
		FireboltProperties props = new FireboltProperties(new Properties());
		props.addProperty("custom_prop", "custom_value", true);
		
		assertEquals(Map.of("custom_prop", "custom_value"), props.getAdditionalProperties());
		
		props.removeProperty("custom_prop");
		
		assertTrue(props.getAdditionalProperties().isEmpty());
	}

	@Test
	void shouldRemoveMultipleCustomProperties() {
		FireboltProperties props = new FireboltProperties(new Properties());
		props.addProperty("prop1", "value1", true);
		props.addProperty("prop2", "value2", true);
		props.addProperty("prop3", "value3", true);
		
		assertEquals(Map.of("prop1", "value1", "prop2", "value2", "prop3", "value3"), props.getAdditionalProperties());
		
		props.removeProperty("prop2");
		assertEquals(Map.of("prop1", "value1", "prop3", "value3"), props.getAdditionalProperties());
		
		props.removeProperty("prop1");
		assertEquals(Map.of("prop3", "value3"), props.getAdditionalProperties());
		
		props.removeProperty("prop3");
		assertTrue(props.getAdditionalProperties().isEmpty());
	}

	@Test
	void shouldHandleRemoveNonExistentProperty() {
		FireboltProperties props = new FireboltProperties(new Properties());
		
		// Should not throw exception when removing non-existent property
		props.removeProperty("non_existent_property");
		
		// Properties should remain unchanged
		assertTrue(props.getAdditionalProperties().isEmpty());
		assertNull(props.getDatabase());
		assertNull(props.getEngine());
		assertNull(props.getTransactionId());
		assertNull(props.getTransactionSequenceId());
	}

	@Test
	void shouldThrowExceptionWhenRemovePropertyWithNullKey() {
		FireboltProperties props = new FireboltProperties(new Properties());
		
		assertThrows(NullPointerException.class, () -> props.removeProperty(null));
	}

}
