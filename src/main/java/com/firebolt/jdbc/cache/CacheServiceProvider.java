package com.firebolt.jdbc.cache;

/**
 * Class responsible for creating the CacheService. We should only create one cache service per JVM, so singleton pattern is implemented
 */
@SuppressWarnings("java:S6548") // suppress the warning for singleton. Yes this is a singleton
public class CacheServiceProvider {

    private static CacheServiceProvider instance;

    private CacheService inMemoryCacheService;
    private CacheService onDiskCacheService;

    // disable creation of the CacheServiceProvider using a constructor from outside this class
    private CacheServiceProvider() {
        this.inMemoryCacheService = new InMemoryCacheService();
        this.onDiskCacheService = new OnDiskMemoryCacheService(inMemoryCacheService);
    }

    public static synchronized CacheServiceProvider getInstance() {
        if (instance == null) {
            instance = new CacheServiceProvider();
        }

        return instance;
    }

    public CacheService getCacheService(CacheType cacheType) throws IllegalArgumentException {
        if (CacheType.IN_MEMORY == cacheType) {
            return inMemoryCacheService;
        } else if (CacheType.ON_DISK == cacheType) {
            return onDiskCacheService;
        }

        throw new IllegalArgumentException("Unknown cache type: " + (cacheType == null ? "null" : cacheType.name()));
    }
}
