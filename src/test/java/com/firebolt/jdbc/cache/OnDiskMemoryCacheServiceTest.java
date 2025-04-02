package com.firebolt.jdbc.cache;

import com.firebolt.jdbc.cache.exception.ConnectionCacheDeserializationException;
import com.firebolt.jdbc.cache.exception.FilenameGenerationException;
import com.firebolt.jdbc.cache.key.CacheKey;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class OnDiskMemoryCacheServiceTest {

    @Mock
    private CacheService mockCacheService;
    @Mock
    private FileService mockFileService;
    @Mock
    private CacheKey mockCacheKey;
    @Mock
    private ConnectionCache mockConnectionCache;
    @Mock
    private OnDiskConnectionCache mockOnDiskConnectionCache;
    @Mock
    private File mockDiskFile;
    @Mock
    private Path mockFilePath;
    @Mock
    private FileTime mockFileTime;

    private OnDiskMemoryCacheService onDiskMemoryCacheService;

    @BeforeEach
    void setupMethod() {
        onDiskMemoryCacheService = new OnDiskMemoryCacheService(mockCacheService, mockFileService);
    }

    @Test
    void willGetValueFromCacheWhenAvailable() {
        when(mockCacheService.get(mockCacheKey)).thenReturn(Optional.of(mockConnectionCache));
        assertSame(mockConnectionCache, onDiskMemoryCacheService.get(mockCacheKey).get());
        verify(mockFileService, never()).findFileForKey(mockCacheKey);
    }

    @Test
    void willNotReturnAnyCacheObjectIfNotInCacheAndFailToReadFromDisk() {
        when(mockCacheService.get(mockCacheKey)).thenReturn(Optional.empty());
        when(mockFileService.findFileForKey(mockCacheKey)).thenThrow(FilenameGenerationException.class);
        assertTrue(onDiskMemoryCacheService.get(mockCacheKey).isEmpty());
    }

    @Test
    void willNotReturnAnyCacheObjectIfNotInCacheAndFileOnDiskDoesNotExist() {
        when(mockCacheService.get(mockCacheKey)).thenReturn(Optional.empty());
        when(mockFileService.findFileForKey(mockCacheKey)).thenReturn(mockDiskFile);
        when(mockDiskFile.exists()).thenReturn(false);
        assertTrue(onDiskMemoryCacheService.get(mockCacheKey).isEmpty());
    }

    @Test
    void willNotReturnAnyCacheObjectIfFoundTheFileOnDiskButCannotDetectItsCreationTime() {
        when(mockCacheService.get(mockCacheKey)).thenReturn(Optional.empty());
        when(mockFileService.findFileForKey(mockCacheKey)).thenReturn(mockDiskFile);
        when(mockDiskFile.toPath()).thenReturn(mockFilePath);
        when(mockDiskFile.exists()).thenReturn(true);

        try (MockedStatic<Files> filesMockedStatic = mockStatic(Files.class)) {
            filesMockedStatic.when(() -> Files.getAttribute(mockFilePath, "basic:creationTime")).thenThrow(IOException.class);
            assertTrue(onDiskMemoryCacheService.get(mockCacheKey).isEmpty());
        }
    }

    @Test
    void willNotReturnAnyCacheObjectIfFoundTheFileOnDiskButWasCreatedTooLongBack() {
        when(mockCacheService.get(mockCacheKey)).thenReturn(Optional.empty());
        when(mockFileService.findFileForKey(mockCacheKey)).thenReturn(mockDiskFile);
        when(mockDiskFile.toPath()).thenReturn(mockFilePath);
        when(mockDiskFile.exists()).thenReturn(true);

        try (MockedStatic<Files> filesMockedStatic = mockStatic(Files.class)) {
            filesMockedStatic.when(() -> Files.getAttribute(mockFilePath, "basic:creationTime")).thenReturn(mockFileTime);
            when(mockFileTime.toInstant()).thenReturn(Instant.now().minus(3, ChronoUnit.HOURS));
            assertTrue(onDiskMemoryCacheService.get(mockCacheKey).isEmpty());
            verify(mockFileService).safelyDeleteFile(mockFilePath);
        }
    }

    @Test
    void willNotReturnAnyCacheObjectIfFoundTheFileOnDiskButCannotReadContent() {
        when(mockCacheService.get(mockCacheKey)).thenReturn(Optional.empty());
        when(mockFileService.findFileForKey(mockCacheKey)).thenReturn(mockDiskFile);
        when(mockDiskFile.toPath()).thenReturn(mockFilePath);
        when(mockDiskFile.exists()).thenReturn(true);

        try (MockedStatic<Files> filesMockedStatic = mockStatic(Files.class)) {
            filesMockedStatic.when(() -> Files.getAttribute(mockFilePath, "basic:creationTime")).thenReturn(mockFileTime);
            when(mockFileTime.toInstant()).thenReturn(Instant.now());
            when(mockFileService.readContent(mockCacheKey, mockDiskFile)).thenReturn(Optional.empty());
            assertTrue(onDiskMemoryCacheService.get(mockCacheKey).isEmpty());
        }
    }

    @Test
    void willNotReturnAnyCacheObjectIfFoundTheFileOnDiskDueToTamperingWithTheFile() {
        when(mockCacheService.get(mockCacheKey)).thenReturn(Optional.empty());
        when(mockFileService.findFileForKey(mockCacheKey)).thenReturn(mockDiskFile);
        when(mockDiskFile.toPath()).thenReturn(mockFilePath);
        when(mockDiskFile.exists()).thenReturn(true);

        try (MockedStatic<Files> filesMockedStatic = mockStatic(Files.class)) {
            filesMockedStatic.when(() -> Files.getAttribute(mockFilePath, "basic:creationTime")).thenReturn(mockFileTime);
            when(mockFileTime.toInstant()).thenReturn(Instant.now());
            when(mockFileService.readContent(mockCacheKey, mockDiskFile)).thenThrow(ConnectionCacheDeserializationException.class);
            assertTrue(onDiskMemoryCacheService.get(mockCacheKey).isEmpty());
            verify(mockFileService).safelyDeleteFile(mockFilePath);
        }
    }

    @Test
    void willReturnCacheObjectFromDisk() {
        when(mockCacheService.get(mockCacheKey)).thenReturn(Optional.empty());
        when(mockFileService.findFileForKey(mockCacheKey)).thenReturn(mockDiskFile);
        when(mockDiskFile.toPath()).thenReturn(mockFilePath);
        when(mockDiskFile.exists()).thenReturn(true);

        try (MockedStatic<Files> filesMockedStatic = mockStatic(Files.class)) {
            filesMockedStatic.when(() -> Files.getAttribute(mockFilePath, "basic:creationTime")).thenReturn(mockFileTime);
            when(mockFileTime.toInstant()).thenReturn(Instant.now());
            when(mockFileService.readContent(mockCacheKey, mockDiskFile)).thenReturn(Optional.of(mockOnDiskConnectionCache));
            assertSame(mockOnDiskConnectionCache, onDiskMemoryCacheService.get(mockCacheKey).get());

            verify(mockOnDiskConnectionCache).setCacheSource(CacheType.DISK.name());
            verify(mockOnDiskConnectionCache).setCacheKey(mockCacheKey);
            verify(mockOnDiskConnectionCache).setOnDiskMemoryCacheService(onDiskMemoryCacheService);

            verify(mockCacheService).put(mockCacheKey, mockOnDiskConnectionCache);
        }
    }

    @Test
    public void savingKeyToMemoryWillAlsoSaveToDisk() {
        onDiskMemoryCacheService.put(mockCacheKey, mockConnectionCache);
        verify(mockCacheService).put(mockCacheKey, mockConnectionCache);
        verify(mockFileService).safeSaveToDiskAsync(mockCacheKey, mockConnectionCache);
    }

}
