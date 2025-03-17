package com.firebolt.jdbc.cache;

import com.firebolt.jdbc.cache.exception.CacheException;
import com.firebolt.jdbc.cache.key.CacheKey;
import java.util.Optional;

/**
 * A cache service that will cache a connection object
 */
public interface CacheService {

    /**
     * Saves an entry in the cache. If there already is an entry for that key it will be overwritten
     *
     * @param key - the key associated with the value to be saved in the cache
     * @param connectionCache - the value to be saved in the cache
     *
     * @throws CacheException - if there is a problem talking to the cache
     */
    void put(CacheKey key, ConnectionCache connectionCache) throws CacheException;

    /**
     * Returns the object from the cache. If the cache is not present then an empty value will be returned.
     * If there is a problem accessing the cache a cacheException will be thrown
     *
     * @param key - the key for which the client is trying to retrive the associated saved value
     * @return
     * @throws CacheException - if there is a problem talking to the cache
     */
    Optional<ConnectionCache> get(CacheKey key) throws CacheException;

}
