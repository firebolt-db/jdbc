package com.firebolt.jdbc.cache;

import com.firebolt.jdbc.cache.exception.ConnectionCacheDeserializationException;
import com.firebolt.jdbc.cache.exception.EncryptionException;
import com.firebolt.jdbc.cache.exception.FilenameGenerationException;
import com.firebolt.jdbc.cache.key.CacheKey;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.lang3.tuple.Pair;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class FileServiceTest {

    private static final String ENCRYPTION_KEY = "some encryption key";
    private static final String FILENAME = "test_file.txt";
    private static final String DIRECTORY = "/tmp/path";
    private static final String ENCRYPTED_CONTENT = "the content of the file encrypted";
    private static final String CONNECTION_ID = "the id of the connection";
    private static final String ACCESS_TOKEN = "the access token";
    private static final String SYSTEM_ENGINE_URL = "https://system.url";
    private static final String DATABASE_NAME = "db1";
    private static final String ENGINE_NAME = "engine1";
    private static final String ENGINE1_URL = "https://my.engine.url";

    @Mock
    private DirectoryPathResolver mockDirectoryPathResolver;
    @Mock
    private FilenameGenerator mockFilenameGenerator;
    @Mock
    private EncryptionService mockEncryptionService;

    @Mock
    private CacheKey mockCacheKey;
    @Mock
    private Path mockPath;
    @Mock
    private File mockFile;

    private ExecutorService executorService = Executors.newFixedThreadPool(1);
    private FileService fileService;

    @Captor
    private ArgumentCaptor<String> connectionCacheJsonArgumentCaptor;

    @BeforeEach
    void initTests() {
        fileService = new FileService(mockDirectoryPathResolver, mockFilenameGenerator, mockEncryptionService, executorService);
    }

    @Test
    void cannotFindFileNameIfFileNameGeneratorThrowsException() {
        when(mockFilenameGenerator.generate(mockCacheKey)).thenThrow(FilenameGenerationException.class);
        assertThrows(FilenameGenerationException.class, () -> fileService.findFileForKey(mockCacheKey));
        verify(mockDirectoryPathResolver, never()).resolveFireboltJdbcDirectory();
    }

    @Test
    void canGetFileNameForCacheKey() {
        when(mockFilenameGenerator.generate(mockCacheKey)).thenReturn(FILENAME);
        when(mockDirectoryPathResolver.resolveFireboltJdbcDirectory()).thenReturn(mockPath);
        when(mockPath.toString()).thenReturn(DIRECTORY);
        File file = fileService.findFileForKey(mockCacheKey);
        assertEquals(DIRECTORY + "/" + FILENAME, file.toString());
    }

    @Test
    void cannotGetContentWhenReadingOfFileThrowsError() {
        when(mockFile.toPath()).thenReturn(mockPath);
        try (MockedStatic<Files> filesMockedStatic = mockStatic(Files.class)) {
            filesMockedStatic.when(() -> Files.readString(mockPath)).thenThrow(IOException.class);
            assertTrue(fileService.readContent(mockCacheKey, mockFile).isEmpty());
            verify(mockEncryptionService, never()).decrypt(any(), any());
        }
    }

    @Test
    void cannotGetContentWhenCannotDecryptContent() {
        when(mockFile.toPath()).thenReturn(mockPath);
        when(mockCacheKey.getEncryptionKey()).thenReturn(ENCRYPTION_KEY);
        try (MockedStatic<Files> filesMockedStatic = mockStatic(Files.class)) {
            filesMockedStatic.when(() -> Files.readString(mockPath)).thenReturn(ENCRYPTED_CONTENT);
            when(mockEncryptionService.decrypt(ENCRYPTED_CONTENT, ENCRYPTION_KEY)).thenThrow(EncryptionException.class);
            assertThrows(ConnectionCacheDeserializationException.class, () -> fileService.readContent(mockCacheKey, mockFile).isEmpty());
        }
    }

    @Test
    void canReadConnectionCacheFromDisk() {
        when(mockFile.toPath()).thenReturn(mockPath);
        when(mockCacheKey.getEncryptionKey()).thenReturn(ENCRYPTION_KEY);
        try (MockedStatic<Files> filesMockedStatic = mockStatic(Files.class)) {
            filesMockedStatic.when(() -> Files.readString(mockPath)).thenReturn(ENCRYPTED_CONTENT);
            String connectionCacheAsString = new JSONObject(actualConnectionCache()).toString();
            when(mockEncryptionService.decrypt(ENCRYPTED_CONTENT, ENCRYPTION_KEY)).thenReturn(connectionCacheAsString);

            assertTrue(fileService.readContent(mockCacheKey, mockFile).isPresent());
        }
    }

    @Test
    void willNotWriteFileToDiskWhenItDoesNotFindTheFile() {
        ConnectionCache connectionCache = actualConnectionCache();
        when(mockFilenameGenerator.generate(mockCacheKey)).thenThrow(FilenameGenerationException.class);

        fileService.safeSaveToDiskAsync(mockCacheKey, connectionCache);

        // sleep so it can execute the task
        sleepForMillis(500);

        verify(mockEncryptionService, never()).encrypt(any(), any());
    }

    @Test
    void willNotWriteFileToDiskWhenCreatingANewFileFails() throws IOException {
        ConnectionCache connectionCache = actualConnectionCache();

        fileService = spy(fileService);
        doReturn(mockFile).when(fileService).findFileForKey(mockCacheKey);
        when(mockFile.exists()).thenReturn(false);
        when(mockFile.createNewFile()).thenThrow(IOException.class);

        fileService.safeSaveToDiskAsync(mockCacheKey, connectionCache);

        // sleep so it can execute the task
        sleepForMillis(500);

        verify(mockEncryptionService, never()).encrypt(any(), any());
    }

    @Test
    void willNotWriteFileToDiskWhenEncryptionFails() {
        ConnectionCache connectionCache = actualConnectionCache();

        when(mockCacheKey.getEncryptionKey()).thenReturn(ENCRYPTION_KEY);
        fileService = spy(fileService);
        doReturn(mockFile).when(fileService).findFileForKey(mockCacheKey);
        when(mockFile.exists()).thenReturn(true);
        when(mockEncryptionService.encrypt(anyString(), eq(ENCRYPTION_KEY))).thenThrow(EncryptionException.class);

        fileService.safeSaveToDiskAsync(mockCacheKey, connectionCache);

        // sleep so it can execute the task
        sleepForMillis(500);

        verify(mockEncryptionService).encrypt(connectionCacheJsonArgumentCaptor.capture(), eq(ENCRYPTION_KEY));

        String jsonConnectionCache = connectionCacheJsonArgumentCaptor.getValue();
        ConnectionCache connectionCache1 = new ConnectionCache(new JSONObject(jsonConnectionCache));

        assertEquals(CONNECTION_ID, connectionCache1.getConnectionId());
        assertEquals(ACCESS_TOKEN, connectionCache1.getAccessToken());
        assertEquals(SYSTEM_ENGINE_URL, connectionCache1.getSystemEngineUrl());

        DatabaseOptions databaseOptions = connectionCache1.getDatabaseOptions(DATABASE_NAME).get();
        assertEquals(1, databaseOptions.getParameters().size());
        assertEquals("database", databaseOptions.getParameters().get(0).getKey());
        assertEquals(DATABASE_NAME, databaseOptions.getParameters().get(0).getValue());

        EngineOptions engineOptions = connectionCache1.getEngineOptions(ENGINE_NAME).get();
        assertEquals(ENGINE1_URL, engineOptions.getEngineUrl());
        assertEquals(1, engineOptions.getParameters().size());
        assertEquals("engine", engineOptions.getParameters().get(0).getKey());
        assertEquals(ENGINE_NAME, engineOptions.getParameters().get(0).getValue());
    }

    @Test
    void willNotWriteFileToDiskWhenWritingToFileEncryptedContentFails() {
        ConnectionCache connectionCache = actualConnectionCache();

        try (MockedStatic<Files> filesMockedStatic = mockStatic(Files.class)) {
            fileService = spy(fileService);
            doReturn(mockFile).when(fileService).findFileForKey(mockCacheKey);
            when(mockFile.exists()).thenReturn(true);
            when(mockEncryptionService.encrypt(anyString(), eq(ENCRYPTION_KEY))).thenReturn(ENCRYPTED_CONTENT);

            filesMockedStatic.when(() -> Files.write(any(Path.class), any(byte[].class))).thenThrow(IOException.class);
            fileService.safeSaveToDiskAsync(mockCacheKey, connectionCache);

            // sleep so it can execute the task
            sleepForMillis(500);
        }
    }

    @Test
    void willWriteToDisk() {
        ConnectionCache connectionCache = actualConnectionCache();

        try (MockedStatic<Files> filesMockedStatic = mockStatic(Files.class)) {
            fileService = spy(fileService);
            doReturn(mockFile).when(fileService).findFileForKey(mockCacheKey);
            when(mockFile.exists()).thenReturn(true);
            when(mockEncryptionService.encrypt(anyString(), eq(ENCRYPTION_KEY))).thenReturn(ENCRYPTED_CONTENT);

            filesMockedStatic.when(() -> Files.write(any(Path.class), any(byte[].class))).thenReturn(mockPath);
            fileService.safeSaveToDiskAsync(mockCacheKey, connectionCache);

            // sleep so it can execute the task
            sleepForMillis(500);
        }
    }

    @Test
    void willNotThrowExceptionIfTryingToDeleteAFileThatDoesNotExist() {
        Path filePathThatDoesNotExist = Path.of("does", "not", "exist");
        fileService.safelyDeleteFile(filePathThatDoesNotExist);
    }

    private void sleepForMillis(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            // do nothing
        }
    }

    private ConnectionCache actualConnectionCache() {
        ConnectionCache connectionCache = new ConnectionCache(CONNECTION_ID);
        connectionCache.setAccessToken(ACCESS_TOKEN);
        connectionCache.setSystemEngineUrl(SYSTEM_ENGINE_URL);
        connectionCache.setDatabaseOptions(DATABASE_NAME, new DatabaseOptions(List.of(Pair.of("database", DATABASE_NAME))));
        connectionCache.setEngineOptions(ENGINE_NAME, new EngineOptions(ENGINE1_URL, List.of(Pair.of("engine", ENGINE_NAME))));
        return connectionCache;
    }


}
