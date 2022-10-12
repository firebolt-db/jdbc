package com.firebolt.jdbc.client;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.lang.reflect.Field;

import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.firebolt.jdbc.connection.settings.FireboltProperties;

class HttpClientConfigTest {

	@BeforeEach
	public void resetSingleton()
			throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
		Field instance = HttpClientConfig.class.getDeclaredField("instance");
		instance.setAccessible(true);
		instance.set(null, null);
	}

	@Test
	void shouldInitHttpClient() throws Exception {
		try (CloseableHttpClient client = HttpClientConfig
				.init(FireboltProperties.builder().maxConnectionsPerRoute(1).maxConnectionsTotal(1).build())) {
			assertNotNull(client);
		}
	}

	@Test
	void shouldBeNullIfClientWasNotInitialized() {
		assertNull(HttpClientConfig.getInstance());
	}
}
