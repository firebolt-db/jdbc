package com.firebolt.jdbc.client;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.lang.reflect.Field;
import java.util.Properties;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.firebolt.jdbc.connection.settings.FireboltProperties;

import okhttp3.OkHttpClient;

class HttpClientConfigTest {

	@BeforeEach
	public void resetSingleton() throws ReflectiveOperationException {
		Field instance = HttpClientConfig.class.getDeclaredField("instance");
		instance.setAccessible(true);
		instance.set(null, null);
	}

	@Test
	void shouldInitHttpClient() throws Exception {
		assertNull(HttpClientConfig.getInstance());
		OkHttpClient client = HttpClientConfig.init(new FireboltProperties(new Properties()));
		assertNotNull(client);
		assertSame(client, HttpClientConfig.getInstance());
	}

	@Test
	void shouldBeNullIfClientWasNotInitialized() {
		assertNull(HttpClientConfig.getInstance());
	}
}
