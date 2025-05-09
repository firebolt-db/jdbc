package com.firebolt.jdbc.cache;

import com.firebolt.jdbc.cache.key.CacheKey;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    @Test
    @SuppressWarnings("java:S2925") // need thread.sleep so we can wait for the cache entry to expire
    void willNotGetExpiredEntryFromCache() {
        ExpiringMap<String, ConnectionCache> expiringMap = ExpiringMap.builder().variableExpiration().build();
        CacheKey cacheKey = new TestCacheKey("key_to_expire");
        expiringMap.put(cacheKey.getValue(), new ConnectionCache("someid"), ExpirationPolicy.CREATED, 500, TimeUnit.MILLISECONDS);

        inMemoryCacheService = new InMemoryCacheService(expiringMap);

        // should get the key
        assertTrue(inMemoryCacheService.get(cacheKey).isPresent());

        // wait for the cache to expire
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            // do nothing
        }

        // should not get the key
        assertTrue(inMemoryCacheService.get(cacheKey).isEmpty());
    }

    @Test
    void willCreateTheInMemoryConnectionCache() {
        CacheKey testCacheKey = new TestCacheKey("key");
        ConnectionCache connectionCache = new ConnectionCache("a sample connection id");
        inMemoryCacheService.put(testCacheKey, connectionCache);
        assertEquals("a sample connection id", connectionCache.getConnectionId());
        assertEquals(CacheType.MEMORY.name(), connectionCache.getCacheSource());
    }

    @Test
    void canRemoveCacheKey() {
        CacheKey testCacheKey = new TestCacheKey("key");
        ConnectionCache connectionCache = new ConnectionCache("a sample connection id");
        inMemoryCacheService.put(testCacheKey, connectionCache);

        Optional<ConnectionCache> connectionCacheOptional = inMemoryCacheService.get(testCacheKey);
        assertEquals("a sample connection id", connectionCacheOptional.get().getConnectionId());

        inMemoryCacheService.remove(testCacheKey);

        connectionCacheOptional = inMemoryCacheService.get(testCacheKey);
        assertTrue(connectionCacheOptional.isEmpty());
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
