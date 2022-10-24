package com.firebolt.jdbc.client;

import com.firebolt.jdbc.connection.settings.FireboltProperties;
import okhttp3.OkHttpClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

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
		OkHttpClient client = HttpClientConfig
				.init(FireboltProperties.builder().maxConnectionsPerRoute(1).maxConnectionsTotal(1).timeToLiveMillis(1).build());
			assertNotNull(client);
	}

	@Test
	void shouldBeNullIfClientWasNotInitialized() {
		assertNull(HttpClientConfig.getInstance());
	}
}
