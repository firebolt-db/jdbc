package com.firebolt.jdbc.cache;

import com.firebolt.jdbc.cache.exception.CacheException;
import com.firebolt.jdbc.cache.key.CacheKey;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.file.Files;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import lombok.CustomLog;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;

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
    private EncryptionService encryptionService;
    private ChecksumGenerator checksumGenerator;
    public OnDiskMemoryCacheService(CacheService cacheService) {
        this(cacheService, new FileService(), new EncryptionService(), new ChecksumGenerator());
    }

    // visible for testing
    OnDiskMemoryCacheService(CacheService cacheService, FileService fileService, EncryptionService encryptionService, ChecksumGenerator checksumGenerator) {
        this.cacheService = cacheService;
        this.fileService = fileService;
        this.encryptionService = encryptionService;
        this.checksumGenerator = checksumGenerator;
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
        Optional<File> cacheFileOptional = fileService.findFileForKey(cacheKey);
        if (cacheFileOptional.isEmpty()) {
            return Optional.empty();
        }

        // found the file, make sure we can still use it
        File cacheFile = cacheFileOptional.get();
        if (isFileTooOld(cacheFile)) {
            // an improvement here is to delete the file since it is already too old
            return Optional.empty();
        }

        // read the value from the file
        Optional<OnDiskConnectionCache> onDiskConnectionCacheOptional = readFileContent(cacheFile);
        if (onDiskConnectionCacheOptional.isEmpty()) {
            return Optional.empty();
        }

        OnDiskConnectionCache onDiskConnectionCache = onDiskConnectionCacheOptional.get();

        // verify the jwt token is correctly encrypted
        Optional<String> jwtToken = encryptionService.decrypt(onDiskConnectionCache.getEncryptedJwtToken(), cacheKey.getEncryptionKey());
        if (jwtToken.isEmpty()) {
            return Optional.empty();
        }

        // can decrypt the token, then check if the checksum is correct
        ConnectionCache connectionCache = asConnectionCache(onDiskConnectionCache, jwtToken.get());
        String fromFileChecksum = onDiskConnectionCache.getChecksum();

        if (StringUtils.isBlank(fromFileChecksum) || !fromFileChecksum.equals(checksumGenerator.generateChecksum(connectionCache))) {
            log.error("Checksum does not match, so cannot use the value from disk");
            return Optional.empty();
        }

        // all good we can use the connection cache from disk
        connectionCache.setCacheSource(CacheType.DISK.name());

        // add it in the memory cache
        cacheService.put(cacheKey, connectionCache);

        return Optional.of(connectionCache);
    }

    private ConnectionCache asConnectionCache(OnDiskConnectionCache onDiskConnectionCache, String decryptedJwtToken) {
        ConnectionCache connectionCache = new ConnectionCache(onDiskConnectionCache.getConnectionId());
        connectionCache.setAccessToken(decryptedJwtToken);
        connectionCache.setSystemEngineUrl(onDiskConnectionCache.getSystemEngineUrl());

        if (onDiskConnectionCache.getDatabaseOptionsMap() != null) {
            onDiskConnectionCache.getDatabaseOptionsMap().entrySet().forEach(databaseEntry -> connectionCache.setDatabaseOptions(databaseEntry.getKey(), databaseEntry.getValue()));
        }

        if (onDiskConnectionCache.getEngineOptionsMap() != null) {
            onDiskConnectionCache.getEngineOptionsMap().entrySet().forEach(engineEntry -> connectionCache.setEngineOptions(engineEntry.getKey(), engineEntry.getValue()));
        }

        return connectionCache;
    }

    private Optional<OnDiskConnectionCache> readFileContent(File cacheFile) {
        String content;
        try {
            content = Files.readString(cacheFile.toPath());
        } catch (IOException e) {
            log.error("Failed to read the contents of the file", e);
            return Optional.empty();
        }

        // convert it into java object
        try {
            Constructor<OnDiskConnectionCache> constructor = OnDiskConnectionCache.class.getDeclaredConstructor(JSONObject.class);
            constructor.setAccessible(true);
            return content == null ? Optional.empty() : Optional.ofNullable(constructor.newInstance(new JSONObject(content)));
        } catch (ReflectiveOperationException | RuntimeException e) {
            log.error("Failed to read the json as connection cache", e);
            return Optional.empty();
        }
    }

    // we should only use the file if it was created within the last 2hours
    private boolean isFileTooOld(File cacheFile) {
        try {
            FileTime creationTime = (FileTime) Files.getAttribute(cacheFile.toPath(), "basic:creationTime");
            return creationTime.toInstant().isBefore(Instant.now().minus(CACHE_TIME_IN_MINUTES, ChronoUnit.MINUTES));
        } catch (IOException e) {
            log.error("Failed to check the creation time of the file");

            // will assume we cannot use the file
            return false;
        }
    }

    private void safelySaveToDiskAsync(CacheKey key, ConnectionCache connectionCache) {
        fileService.save(key, connectionCache);
    }
}
