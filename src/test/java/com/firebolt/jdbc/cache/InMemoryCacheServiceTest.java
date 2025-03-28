package com.firebolt.jdbc.cache;

import com.firebolt.jdbc.cache.key.CacheKey;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@ExtendWith(MockitoExtension.class)
class InMemoryCacheServiceTest {

    private InMemoryCacheService inMemoryCacheService;

    @Mock
    private ConnectionCache mockConnectionCache;

    @BeforeEach
    void init() {
        inMemoryCacheService = new InMemoryCacheService();
    }

    @Test
    void willReturnEmptyIfKeyIsNotInCache() {
        CacheKey cacheKey = new TestCacheKey("not_existing_key");
        Optional<ConnectionCache> keyNotPresent = inMemoryCacheService.get(cacheKey);
        assertTrue(keyNotPresent.isEmpty());
    }

    @Test
    void canSaveCacheKeyAndGetIt() {
        CacheKey cacheKey = new TestCacheKey("sampleKey");
        Optional<ConnectionCache> connectionCacheOptional = inMemoryCacheService.get(cacheKey);
        assertTrue(connectionCacheOptional.isEmpty());

        inMemoryCacheService.put(cacheKey, mockConnectionCache);

        connectionCacheOptional = inMemoryCacheService.get(cacheKey);
        assertSame(connectionCacheOptional.get(), mockConnectionCache);
    }

    private class TestCacheKey implements CacheKey {

        private String keyValue;

        public TestCacheKey(String keyValue) {
            this.keyValue = keyValue;
        }

        @Override
        public String getValue() {
            return keyValue;
        }

        @Override
        public String getEncryptionKey() {
            return "some encryption key";
        }
    }
}
