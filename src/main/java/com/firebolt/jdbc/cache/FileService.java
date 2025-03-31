package com.firebolt.jdbc.cache;

import com.firebolt.jdbc.cache.exception.EncryptionException;
import com.firebolt.jdbc.cache.exception.FilenameGenerationException;
import com.firebolt.jdbc.cache.key.CacheKey;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.CustomLog;
import org.json.JSONObject;

@CustomLog
public class FileService {

    private DirectoryPathResolver directoryPathResolver;
    private FilenameGenerator filenameGenerator;
    private ExecutorService executorService;
    private EncryptionService encryptionService;

    private static FileService instance;


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
        Path fireboltJdbcDriverFolder = directoryPathResolver.resolveFireboltJdbcDirectory();

        return new File(Paths.get(fireboltJdbcDriverFolder.toString(), filenameForCache).toString());
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
                    file.createNewFile();
                } catch (IOException e) {
                    // maybe do not have permission to write to that location
                    log.error("Cannot create on-disk connection cache. Maybe do not have the write permission.");
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
                log.error("Failed to encrypt the connection cache so will not save it do disk.");
                return;
            }

            try {
                Files.write(file.toPath(), encryptedConnectionCache.getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                log.error("Failed to write to cache");
            }

        });
    }

    public Optional<ConnectionCache> readContent(CacheKey cacheKey, File cacheFile) {
        String content;
        try {
            content = Files.readString(cacheFile.toPath());
        } catch (IOException e) {
            log.error("Failed to read the contents of the file", e);
            return Optional.empty();
        }

        // decrypt the content
        String decryptedCacheObject;
        try {
            decryptedCacheObject = encryptionService.decrypt(content, cacheKey.getEncryptionKey());
        } catch (EncryptionException e) {
            log.error("Cannot decrypt cache content file.");
            return Optional.empty();
        }

        // convert to ConnectionCache
        return Optional.of(new ConnectionCache(new JSONObject(decryptedCacheObject)));
    }

}
