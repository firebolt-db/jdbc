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

    /**
     * This will be used when saving to disk to encrypt the jwt token and to obfuscate the file name where it is saved.
     * @return
     */
    String getEncryptionKey();
}
