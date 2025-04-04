package com.firebolt.jdbc.cache;

import com.firebolt.jdbc.cache.key.CacheKey;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class OnDiskConnectionCacheTest {

    private static final String CONNECTION_ID = " a connection id";
    private static final String ACCESS_TOKEN = "the access token";
    private static final String SYSTEM_ENGINE_URL = "the system engine url";

    private static final String DATABASE_NAME = "a database";
    private static final String ENGINE_NAME = "an engine";

    @Mock
    private CacheKey mockCacheKey;

    @Mock
    private OnDiskMemoryCacheService mockOnDiskMemoryCacheService;

    @Mock
    private DatabaseOptions mockDatabaseOptions;

    @Mock
    private EngineOptions mockEngineOptions;

    private OnDiskConnectionCache onDiskConnectionCache;

    @BeforeEach
    void setupTest() {
        onDiskConnectionCache = new OnDiskConnectionCache(CONNECTION_ID);
        onDiskConnectionCache.setCacheKey(mockCacheKey);
        onDiskConnectionCache.setOnDiskMemoryCacheService(mockOnDiskMemoryCacheService);
    }

    @Test
    void canSetJwtToken() {
        onDiskConnectionCache.setAccessToken(ACCESS_TOKEN);

        assertEquals(ACCESS_TOKEN, onDiskConnectionCache.getAccessToken());
        verify(mockOnDiskMemoryCacheService).safelySaveToDiskAsync(mockCacheKey, onDiskConnectionCache);
    }

    @Test
    void canSetSystemEngineUrl() {
        onDiskConnectionCache.setSystemEngineUrl(SYSTEM_ENGINE_URL);

        assertEquals(SYSTEM_ENGINE_URL, onDiskConnectionCache.getSystemEngineUrl());
        verify(mockOnDiskMemoryCacheService).safelySaveToDiskAsync(mockCacheKey, onDiskConnectionCache);
    }

    @Test
    void canSetDatabaseOptions() {
        onDiskConnectionCache.setDatabaseOptions(DATABASE_NAME, mockDatabaseOptions);
        assertSame(mockDatabaseOptions, onDiskConnectionCache.getDatabaseOptions(DATABASE_NAME).get());
        verify(mockOnDiskMemoryCacheService).safelySaveToDiskAsync(mockCacheKey, onDiskConnectionCache);
    }

    @Test
    void canSetEngineOptions() {
        onDiskConnectionCache.setEngineOptions(ENGINE_NAME, mockEngineOptions);
        assertSame(mockEngineOptions, onDiskConnectionCache.getEngineOptions(ENGINE_NAME).get());
        verify(mockOnDiskMemoryCacheService).safelySaveToDiskAsync(mockCacheKey, onDiskConnectionCache);
    }

}
