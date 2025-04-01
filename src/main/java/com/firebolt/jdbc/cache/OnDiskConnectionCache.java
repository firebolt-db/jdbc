package com.firebolt.jdbc.cache;

import com.firebolt.jdbc.cache.key.CacheKey;
import org.json.JSONObject;

public class OnDiskConnectionCache extends ConnectionCache {

    /**
     * When the object is saved, also save it do disk
     */
    private OnDiskMemoryCacheService onDiskMemoryCacheService;

    /**
     * Keep track of the cache key so we can save to disk
     */
    private CacheKey cacheKey;

    public OnDiskConnectionCache(JSONObject jsonObject) {
        super(jsonObject);
    }

    public OnDiskConnectionCache(String connectionId) {
        super(connectionId);
    }

    public void setCacheKey(CacheKey cacheKey) {
        this.cacheKey = cacheKey;
    }

    public void setOnDiskMemoryCacheService(OnDiskMemoryCacheService onDiskMemoryCacheService) {
        this.onDiskMemoryCacheService = onDiskMemoryCacheService;
    }

    @Override
    public void setAccessToken(String accessToken) {
        super.setAccessToken(accessToken);
        onDiskMemoryCacheService.safelySaveToDiskAsync(cacheKey, this);
    }

    @Override
    public void setSystemEngineUrl(String systemEngineUrl) {
        super.setSystemEngineUrl(systemEngineUrl);
        onDiskMemoryCacheService.safelySaveToDiskAsync(cacheKey, this);
    }

    @Override
    public void setEngineOptions(String engineName, EngineOptions engineOptions) {
        super.setEngineOptions(engineName, engineOptions);
        onDiskMemoryCacheService.safelySaveToDiskAsync(cacheKey, this);
    }

    @Override
    public void setDatabaseOptions(String databaseName, DatabaseOptions databaseOptions) {
        super.setDatabaseOptions(databaseName, databaseOptions);
        onDiskMemoryCacheService.safelySaveToDiskAsync(cacheKey, this);
    }

}
