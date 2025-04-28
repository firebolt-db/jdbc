package com.firebolt.jdbc.cache;

import com.firebolt.jdbc.cache.exception.ConnectionCacheDeserializationException;
import com.firebolt.jdbc.cache.exception.FilenameGenerationException;
import com.firebolt.jdbc.cache.key.CacheKey;
import java.io.File;
import java.nio.file.Path;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OnDiskMemoryCacheServiceTest {

    @Mock
    private InMemoryCacheService mockInMemoryCacheService;
    @Mock
    private FileService mockFileService;
    @Mock
    private CacheKey mockCacheKey;
    @Mock
    private ConnectionCache mockConnectionCache;
    @Mock
    private File mockDiskFile;
    @Mock
    private Path mockFilePath;

    private OnDiskMemoryCacheService onDiskMemoryCacheService;

    @BeforeEach
    void setupMethod() {
        onDiskMemoryCacheService = new OnDiskMemoryCacheService(mockInMemoryCacheService, mockFileService);
    }

    @Test
    void willGetValueFromCacheWhenAvailable() {
        when(mockInMemoryCacheService.get(mockCacheKey)).thenReturn(Optional.of(mockConnectionCache));
        assertSame(mockConnectionCache, onDiskMemoryCacheService.get(mockCacheKey).get());
        verify(mockFileService, never()).findFileForKey(mockCacheKey);
    }

    @Test
    void willNotReturnAnyCacheObjectIfNotInCacheAndFailToReadFromDisk() {
        when(mockInMemoryCacheService.get(mockCacheKey)).thenReturn(Optional.empty());
        when(mockFileService.findFileForKey(mockCacheKey)).thenThrow(FilenameGenerationException.class);
        assertTrue(onDiskMemoryCacheService.get(mockCacheKey).isEmpty());
    }

    @Test
    void willNotReturnAnyCacheObjectIfNotInCacheAndFileOnDiskDoesNotExist() {
        when(mockInMemoryCacheService.get(mockCacheKey)).thenReturn(Optional.empty());
        when(mockFileService.findFileForKey(mockCacheKey)).thenReturn(mockDiskFile);
        when(mockDiskFile.exists()).thenReturn(false);
        assertTrue(onDiskMemoryCacheService.get(mockCacheKey).isEmpty());
    }

    @Test
    void willNotReturnAnyCacheObjectIfFoundTheFileOnDiskButWasCreatedTooLongBack() {
        when(mockInMemoryCacheService.get(mockCacheKey)).thenReturn(Optional.empty());
        when(mockFileService.findFileForKey(mockCacheKey)).thenReturn(mockDiskFile);
        when(mockDiskFile.toPath()).thenReturn(mockFilePath);
        when(mockDiskFile.exists()).thenReturn(true);
        when(mockFileService.wasFileCreatedBeforeTimestamp(mockDiskFile, OnDiskMemoryCacheService.CACHE_TIME_IN_MINUTES, ChronoUnit.MINUTES)).thenReturn(true);

        assertTrue(onDiskMemoryCacheService.get(mockCacheKey).isEmpty());
        verify(mockFileService).safelyDeleteFile(mockFilePath);
    }

    @Test
    void willNotReturnAnyCacheObjectIfFoundTheFileOnDiskButCannotReadContent() {
        when(mockInMemoryCacheService.get(mockCacheKey)).thenReturn(Optional.empty());
        when(mockFileService.findFileForKey(mockCacheKey)).thenReturn(mockDiskFile);
        when(mockDiskFile.exists()).thenReturn(true);
        when(mockFileService.wasFileCreatedBeforeTimestamp(mockDiskFile, OnDiskMemoryCacheService.CACHE_TIME_IN_MINUTES, ChronoUnit.MINUTES)).thenReturn(false);
        when(mockFileService.readContent(mockCacheKey, mockDiskFile)).thenReturn(Optional.empty());
        assertTrue(onDiskMemoryCacheService.get(mockCacheKey).isEmpty());
    }

    @Test
    void willNotReturnAnyCacheObjectIfFoundTheFileOnDiskDueToTamperingWithTheFile() {
        when(mockInMemoryCacheService.get(mockCacheKey)).thenReturn(Optional.empty());
        when(mockFileService.findFileForKey(mockCacheKey)).thenReturn(mockDiskFile);
        when(mockDiskFile.toPath()).thenReturn(mockFilePath);
        when(mockDiskFile.exists()).thenReturn(true);
        when(mockFileService.wasFileCreatedBeforeTimestamp(mockDiskFile, OnDiskMemoryCacheService.CACHE_TIME_IN_MINUTES, ChronoUnit.MINUTES)).thenReturn(false);

        when(mockFileService.readContent(mockCacheKey, mockDiskFile)).thenThrow(ConnectionCacheDeserializationException.class);
        assertTrue(onDiskMemoryCacheService.get(mockCacheKey).isEmpty());
        verify(mockFileService).safelyDeleteFile(mockFilePath);
    }

    @Test
    void willReturnCacheObjectFromDisk() {
        when(mockInMemoryCacheService.get(mockCacheKey)).thenReturn(Optional.empty());
        when(mockFileService.findFileForKey(mockCacheKey)).thenReturn(mockDiskFile);
        when(mockDiskFile.exists()).thenReturn(true);
        when(mockFileService.wasFileCreatedBeforeTimestamp(mockDiskFile, OnDiskMemoryCacheService.CACHE_TIME_IN_MINUTES, ChronoUnit.MINUTES)).thenReturn(false);

        when(mockFileService.readContent(mockCacheKey, mockDiskFile)).thenReturn(Optional.of(mockConnectionCache));
        assertSame(mockConnectionCache, onDiskMemoryCacheService.get(mockCacheKey).get());

        verify(mockConnectionCache).setCacheSource(CacheType.DISK.name());

        verify(mockInMemoryCacheService).put(mockCacheKey, mockConnectionCache);
    }

    @Test
    void savingKeyToMemoryWillAlsoSaveToDisk() {
        onDiskMemoryCacheService.put(mockCacheKey, mockConnectionCache);
        verify(mockInMemoryCacheService).put(mockCacheKey, mockConnectionCache);
        verify(mockFileService).safeSaveToDiskAsync(mockCacheKey, mockConnectionCache);
    }

    @Test
    void canRemoveCacheKey() {
        when(mockFileService.findFileForKey(mockCacheKey)).thenReturn(mockDiskFile);
        when(mockDiskFile.toPath()).thenReturn(mockFilePath);
        when(mockDiskFile.exists()).thenReturn(true);

        onDiskMemoryCacheService.remove(mockCacheKey);
        verify(mockInMemoryCacheService).remove(mockCacheKey);
        verify(mockFileService).safelyDeleteFile(mockFilePath);
    }

}
