package com.firebolt.jdbc.cache;

import com.firebolt.jdbc.cache.exception.CacheException;
import com.firebolt.jdbc.cache.key.CacheKey;
import java.util.Optional;

/**
 * A wrapper on another cache service, that will check the disk if the cached object is not present in the wrapped cached service
 *
 * Keep it package protected as only the CacheServiceProvider class should create it
 */
class OnDiskMemoryCacheService implements CacheService {

    private CacheService cacheService;

    public OnDiskMemoryCacheService(CacheService cacheService) {
        this.cacheService = cacheService;
    }


    @Override
    public void put(CacheKey key, ConnectionCache connectionCache) throws CacheException {
        cacheService.put(key, connectionCache);

        // also save to disk. to be implemented
    }

    @Override
    public Optional<ConnectionCache> get(CacheKey key) throws CacheException {
        // first check if it is in the memory cache
        Optional<ConnectionCache> connectionCacheOptional = cacheService.get(key);
        if (connectionCacheOptional.isPresent()) {
            return connectionCacheOptional;
        }
        // to be implemented to get from disk
        return Optional.empty();
    }
}
