package com.firebolt.jdbc.cache.key;


/**
 * Implementations should make sure that the CacheKey has equals and hash methods implemented
 */
public interface CacheKey {

    /**
     * Returns the cache key value
     * @return
     */
    String getValue();
}
