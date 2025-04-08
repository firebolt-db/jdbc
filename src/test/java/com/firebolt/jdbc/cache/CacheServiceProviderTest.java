package com.firebolt.jdbc.cache;


import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CacheServiceProviderTest {

    @Test
    void willReturnTheSameInstanceOfCacheProvider() {
        CacheServiceProvider cacheServiceProvider1 = CacheServiceProvider.getInstance();
        CacheServiceProvider cacheServiceProvider2 = CacheServiceProvider.getInstance();
        assertSame(cacheServiceProvider1, cacheServiceProvider2);
    }

    @Test
    void willReturnTheSameCacheServiceEveryTime() {
        CacheServiceProvider cacheServiceProvider = CacheServiceProvider.getInstance();
        CacheService cacheService1 = cacheServiceProvider.getCacheService(CacheType.MEMORY);
        CacheService cacheService2 = cacheServiceProvider.getCacheService(CacheType.MEMORY);
        assertSame(cacheService1, cacheService2);
        assertInstanceOf(InMemoryCacheService.class, cacheService1);
    }

    @Test
    void cannotCreateCacheServiceForNullCacheType() {
        CacheServiceProvider cacheServiceProvider = CacheServiceProvider.getInstance();
        assertThrows(IllegalArgumentException.class, () -> cacheServiceProvider.getCacheService(null));
    }
}
