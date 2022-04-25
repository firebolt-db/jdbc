package io.firebolt.jdbc.client;

import io.firebolt.jdbc.connection.settings.FireboltProperties;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class HttpClientConfigTest {

    @BeforeEach
    public void resetSingleton() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
        Field instance = HttpClientConfig.class.getDeclaredField("client");
        instance.setAccessible(true);
        instance.set(null, null);
    }

    @Test
    void shouldInitHttpClient() throws Exception {
        try (CloseableHttpClient client = HttpClientConfig.init(FireboltProperties.builder().defaultMaxPerRoute(1).maxTotal(1).build())) {
            assertNotNull(client);
        }
    }

    @Test
    void shouldBeNullIfClientWasNotInitialized() {
        assertNull(HttpClientConfig.getInstance());
    }
}