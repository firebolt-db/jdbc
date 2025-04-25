package com.firebolt.jdbc.cache;

import com.firebolt.jdbc.annotation.ExcludeFromJacocoGeneratedReport;
import com.firebolt.jdbc.cache.exception.ConnectionCacheDeserializationException;
import com.firebolt.jdbc.cache.exception.EncryptionException;
import com.firebolt.jdbc.cache.exception.FilenameGenerationException;
import com.firebolt.jdbc.cache.key.CacheKey;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.CustomLog;
import org.json.JSONObject;

@CustomLog
public class FileService {

    static final String CREATION_TIME_FILE_ATTRIBUTE = "basic:creationTime";

    private DirectoryPathResolver directoryPathResolver;
    private FilenameGenerator filenameGenerator;
    private ExecutorService executorService;
    private EncryptionService encryptionService;

    private static FileService instance;

    private Path fireboltJdbcDirectory;

    @ExcludeFromJacocoGeneratedReport
    public static FileService getInstance() {
        if (instance == null) {
            instance = new FileService(new DirectoryPathResolver(), new FilenameGenerator(), new EncryptionService(), Executors.newFixedThreadPool(2));
        }
        return instance;
    }

    // visible for testing
    FileService(DirectoryPathResolver directoryPathResolver, FilenameGenerator filenameGenerator, EncryptionService encryptionService, ExecutorService executorService) {
        this.directoryPathResolver = directoryPathResolver;
        this.filenameGenerator = filenameGenerator;
        this.encryptionService = encryptionService;
        this.executorService = executorService;
    }

    /**
     * Returns true if the file exists. False otherwise
     *
     * @param cacheKey - the key for which we are looking for the file
     * @return
     */
    public File findFileForKey(CacheKey cacheKey) throws FilenameGenerationException {
        // generate the file name
        String filenameForCache = filenameGenerator.generate(cacheKey);

        // will write the filename in the firebolt jdbc driver
        if (fireboltJdbcDirectory == null) {
            fireboltJdbcDirectory = directoryPathResolver.resolveFireboltJdbcDirectory();
        }

        return new File(Paths.get(fireboltJdbcDirectory.toString(), filenameForCache).toString());
    }

    public void safeSaveToDiskAsync(CacheKey cacheKey, ConnectionCache connectionCache) {
        executorService.submit(() -> {
            // check if a file exists
            File file;
            try {
                file = findFileForKey(cacheKey);
            } catch (FilenameGenerationException e) {
                // if we cannot generate the file name, we cannot save the content
                log.warn("Cannot save the cache connection to disk.");
                return;
            }

            if (!file.exists()) {
                log.debug("Creating a new file for on disk caching");
                try {
                    boolean fileCreated = file.createNewFile();
                    if (!fileCreated) {
                        log.warn("Cannot create file to save the connection cache.");
                        return;
                    }
                } catch (IOException e) {
                    // maybe do not have permission to write to that location
                    log.warn("Cannot create on-disk connection cache. Maybe do not have the write permission. ", e);
                    return;
                }
            }

            // convert to json and encrypt it
            JSONObject jsonObject = new JSONObject(connectionCache);

            // remove the cache type, as we don't need to serialize it
            jsonObject.remove("cacheSource");

            String encryptedConnectionCache;
            try {
                encryptedConnectionCache = encryptionService.encrypt(jsonObject.toString(), cacheKey.getEncryptionKey());
            } catch (EncryptionException e) {
                log.warn("Failed to encrypt the connection cache so will not save it do disk.");
                return;
            }

            // safely write the content
            safelyWriteFile(file, encryptedConnectionCache);

        });
    }

    // keep it package protected. We had to take it outside as mockito static does not work in different executor contexts
    void safelyWriteFile(File file, String encryptedConnectionCache) {
        try {
            // get the original create time so we don't override it
            FileTime creationTime = (FileTime) Files.getAttribute(file.toPath(), CREATION_TIME_FILE_ATTRIBUTE);

            // overwrite the existing file
            Files.writeString(file.toPath(), encryptedConnectionCache, StandardOpenOption.TRUNCATE_EXISTING);

            // Restore the original creation time
            Files.setAttribute(file.toPath(), CREATION_TIME_FILE_ATTRIBUTE, creationTime);
        } catch (IOException e) {
            log.warn("Failed to write to cache");
        }
    }

    public Optional<ConnectionCache> readContent(CacheKey cacheKey, File cacheFile) throws ConnectionCacheDeserializationException {
        String content;
        try {
            content = Files.readString(cacheFile.toPath());
        } catch (IOException e) {
            log.warn("Failed to read the contents of the file", e);
            return Optional.empty();
        }

        // decrypt the content
        String decryptedCacheObject;
        try {
            decryptedCacheObject = encryptionService.decrypt(content, cacheKey.getEncryptionKey());
        } catch (EncryptionException e) {
            log.warn("Cannot decrypt cache content file.");
            throw new ConnectionCacheDeserializationException();
        }

        // convert to ConnectionCache
        return Optional.of(new ConnectionCache(new JSONObject(decryptedCacheObject)));
    }

    public void safelyDeleteFile(Path filePath) {
        try {
            Files.deleteIfExists(filePath);
        } catch (IOException e) {
            log.warn("Failed to delete the cache file", e);
        }
    }

    /**
     * From the current time we subtract the value passed in as parameter and compare it against the file creation time from disk
     * The assumption is that the file exists.
     *
     * @return - true if the file was created before the specified time
     */
    public boolean wasFileCreatedBeforeTimestamp(File file, long value, ChronoUnit timeUnit) {
        try {
            FileTime creationTime = (FileTime) Files.getAttribute(file.toPath(), FileService.CREATION_TIME_FILE_ATTRIBUTE);
            return creationTime.toInstant().isBefore(Instant.now().minus(value, timeUnit));
        } catch (IOException e) {
            log.warn("Failed to check the creation time of the file", e);

            // will assume we cannot use the file
            return true;
        }
    }
}
