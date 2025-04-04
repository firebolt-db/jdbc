package com.firebolt.jdbc.cache;

import com.firebolt.jdbc.cache.exception.CacheException;
import com.firebolt.jdbc.cache.exception.ConnectionCacheDeserializationException;
import com.firebolt.jdbc.cache.exception.FilenameGenerationException;
import com.firebolt.jdbc.cache.key.CacheKey;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
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

    // the jwt token is only valid for 2hours. So keep the cache files for 10 mins less
    private static final int CACHE_TIME_IN_MINUTES = 110;

    // this would be the in memory cache
    private CacheService cacheService;
    private FileService fileService;

    public OnDiskMemoryCacheService(CacheService cacheService) {
        this(cacheService, FileService.getInstance());
    }

    // visible for testing
    OnDiskMemoryCacheService(CacheService cacheService, FileService fileService) {
        this.cacheService = cacheService;
        this.fileService = fileService;
    }

    @Override
    public void put(CacheKey key, ConnectionCache connectionCache) throws CacheException {
        cacheService.put(key, connectionCache);

        // also save to disk in async manner
        safelySaveToDiskAsync(key, connectionCache);
    }

    @Override
    public Optional<ConnectionCache> get(CacheKey cacheKey) throws CacheException {
        // first check if it is in the memory cache
        Optional<ConnectionCache> connectionCacheOptional = cacheService.get(cacheKey);
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
        Optional<OnDiskConnectionCache> onDiskConnectionCacheOptional = readConnectionCacheObjectFromDisk(cacheKey, cacheFile);
        if (onDiskConnectionCacheOptional.isEmpty()) {
            return Optional.empty();
        }

        OnDiskConnectionCache onDiskConnectionCache = onDiskConnectionCacheOptional.get();

        // add it in the memory cache
        cacheService.put(cacheKey, onDiskConnectionCache);

        return Optional.of(onDiskConnectionCache);
    }

    /**
     * Reads the connection cache object from disk. If cannot deserialize the object from file content, it means that the file was corrupted and we can delete it.
     */
    private Optional<OnDiskConnectionCache> readConnectionCacheObjectFromDisk(CacheKey cacheKey, File cacheFile) {
        Optional<OnDiskConnectionCache> cacheConnectionFromDiskOptional;
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

        OnDiskConnectionCache onDiskConnectionCache = cacheConnectionFromDiskOptional.get();

        // all good we can use the connection cache from disk
        onDiskConnectionCache.setCacheSource(CacheType.DISK.name());
        onDiskConnectionCache.setCacheKey(cacheKey);
        onDiskConnectionCache.setOnDiskMemoryCacheService(this);

        return Optional.of(onDiskConnectionCache);
    }

    @Override
    public ConnectionCache newCacheObject(CacheKey cacheKey, String connectionId) {
        OnDiskConnectionCache connectionCache = new OnDiskConnectionCache(connectionId);
        connectionCache.setCacheKey(cacheKey);
        connectionCache.setOnDiskMemoryCacheService(this);
        return connectionCache;
    }

    // we should only use the file if it was created within the last 2hours
    private boolean isFileTooOld(File cacheFile) {
        try {
            FileTime creationTime = (FileTime) Files.getAttribute(cacheFile.toPath(), "basic:creationTime");
            return creationTime.toInstant().isBefore(Instant.now().minus(CACHE_TIME_IN_MINUTES, ChronoUnit.MINUTES));
        } catch (IOException e) {
            log.error("Failed to check the creation time of the file", e);

            // will assume we cannot use the file
            return true;
        }
    }

    public void safelySaveToDiskAsync(CacheKey key, ConnectionCache connectionCache) {
        fileService.safeSaveToDiskAsync(key, connectionCache);
    }
}
