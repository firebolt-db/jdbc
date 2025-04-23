package com.firebolt.jdbc.cache;

import com.firebolt.jdbc.cache.exception.CacheException;
import com.firebolt.jdbc.cache.key.CacheKey;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;

/**
 * Cache service that uses the memory to store the cached values.
 *
 * NOTE: this should be package protected as the clients should use the {@link CacheServiceProvider} to create a cache service.
 */
class InMemoryCacheService implements CacheService {

    // by default cache the connection for 1hr
    private static final int DEFAULT_CACHE_TTL_IN_HOURS = 1;

    private ExpiringMap<String, ConnectionCache> map;

    public InMemoryCacheService() {
        this(ExpiringMap.builder().variableExpiration().build());
    }

    // visible for testing
    InMemoryCacheService(ExpiringMap<String, ConnectionCache> map) {
        this.map = map;
    }

    @Override
    public void put(CacheKey key, ConnectionCache connectionCache) throws CacheException {
        connectionCache.setCacheSource(CacheType.MEMORY.name());
        map.put(key.getValue(), connectionCache, ExpirationPolicy.CREATED, DEFAULT_CACHE_TTL_IN_HOURS, TimeUnit.HOURS);
    }

    @Override
    public Optional<ConnectionCache> get(CacheKey key) throws CacheException {
        return Optional.ofNullable(map.get(key.getValue()));
    }

}
