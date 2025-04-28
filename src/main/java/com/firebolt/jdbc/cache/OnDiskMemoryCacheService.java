package com.firebolt.jdbc.cache;

import com.firebolt.jdbc.cache.exception.CacheException;
import com.firebolt.jdbc.cache.exception.ConnectionCacheDeserializationException;
import com.firebolt.jdbc.cache.exception.FilenameGenerationException;
import com.firebolt.jdbc.cache.key.CacheKey;
import java.io.File;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import lombok.CustomLog;

/**
 * A wrapper on another cache service, that will check the disk if the cached object is not present in the wrapped cached service
 *
 * Keep it package protected as only the CacheServiceProvider class should create it
 */
@CustomLog
class OnDiskMemoryCacheService implements CacheService {

    // the jwt token is only valid for 2hours, but we will keep the on disk cache for only 1 hr.
    static final int CACHE_TIME_IN_MINUTES = 60;

    // this would be the in memory cache
    private InMemoryCacheService inMemoryCacheService;
    private FileService fileService;

    public OnDiskMemoryCacheService(InMemoryCacheService inMemoryCacheService) {
        this(inMemoryCacheService, FileService.getInstance());
    }

    // visible for testing
    OnDiskMemoryCacheService(InMemoryCacheService inMemoryCacheService, FileService fileService) {
        this.inMemoryCacheService = inMemoryCacheService;
        this.fileService = fileService;
    }

    @Override
    public void put(CacheKey key, ConnectionCache connectionCache) throws CacheException {
        inMemoryCacheService.put(key, connectionCache);

        // also save to disk in async manner
        safelySaveToDiskAsync(key, connectionCache);
    }

    @Override
    public Optional<ConnectionCache> get(CacheKey cacheKey) throws CacheException {
        // first check if it is in the memory cache
        Optional<ConnectionCache> connectionCacheOptional = inMemoryCacheService.get(cacheKey);
        if (connectionCacheOptional.isPresent()) {
            // make sure when we read it we set the cache source as memory. When we save it from disk we set it to disk
            connectionCacheOptional.get().setCacheSource(CacheType.MEMORY.name());
            return connectionCacheOptional;
        }

        // try to get it from disk
        File cacheFile;
        try {
            cacheFile = fileService.findFileForKey(cacheKey);
        } catch (FilenameGenerationException e) {
            log.error("Failed to generate the file name");
            return Optional.empty();
        }

        if (!cacheFile.exists()) {
            log.debug("Cache file does not exist");
            return Optional.empty();
        }

        // found the file, make sure we can still use it
        if (isFileTooOld(cacheFile)) {
            fileService.safelyDeleteFile(cacheFile.toPath());
            return Optional.empty();
        }

        // read the value from the file
        Optional<ConnectionCache> onDiskConnectionCacheOptional = readConnectionCacheObjectFromDisk(cacheKey, cacheFile);
        if (onDiskConnectionCacheOptional.isEmpty()) {
            return Optional.empty();
        }

        ConnectionCache onDiskConnectionCache = onDiskConnectionCacheOptional.get();

        // add it in the memory cache
        inMemoryCacheService.put(cacheKey, onDiskConnectionCache);

        return Optional.of(onDiskConnectionCache);
    }

    @Override
    public void remove(CacheKey key) {
        // remove it from memory
        inMemoryCacheService.remove(key);

        // remove it from disk
        File cacheFile;
        try {
            cacheFile = fileService.findFileForKey(key);
        } catch (FilenameGenerationException e) {
            log.error("Failed to generate the file name for key, so cannot remove it");
            return;
        }

        if (!cacheFile.exists()) {
            log.debug("Cache file does not exist");
            return;
        }

        // found the file, make sure we can still use it
        fileService.safelyDeleteFile(cacheFile.toPath());
    }

    /**
     * Reads the connection cache object from disk. If we cannot deserialize the object from file content, it means that the file was corrupted and we can delete it.
     */
    private Optional<ConnectionCache> readConnectionCacheObjectFromDisk(CacheKey cacheKey, File cacheFile) {
        Optional<ConnectionCache> cacheConnectionFromDiskOptional;
        try {
            cacheConnectionFromDiskOptional = fileService.readContent(cacheKey, cacheFile);
        } catch (ConnectionCacheDeserializationException e) {
            // when cannot read from file, we can delete the file since the file seems to be corrupted anyhow
            fileService.safelyDeleteFile(cacheFile.toPath());

            return Optional.empty();
        }

        if (cacheConnectionFromDiskOptional.isEmpty()) {
            return Optional.empty();
        }

        ConnectionCache onDiskConnectionCache = cacheConnectionFromDiskOptional.get();

        // all good we can use the connection cache from disk
        onDiskConnectionCache.setCacheSource(CacheType.DISK.name());
        return Optional.of(onDiskConnectionCache);
    }

    // we should only use the file if it was created within the last hour
    private boolean isFileTooOld(File cacheFile) {
        return fileService.wasFileCreatedBeforeTimestamp(cacheFile, CACHE_TIME_IN_MINUTES, ChronoUnit.MINUTES);
    }

    public void safelySaveToDiskAsync(CacheKey key, ConnectionCache connectionCache) {
        fileService.safeSaveToDiskAsync(key, connectionCache);
    }
}
