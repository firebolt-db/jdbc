package com.firebolt.jdbc.cache;

import com.firebolt.jdbc.cache.exception.EncryptionException;
import com.firebolt.jdbc.cache.exception.FilenameGenerationException;
import com.firebolt.jdbc.cache.key.CacheKey;
import lombok.CustomLog;

/**
 * Generates a file name that should be unique for a cache key using an argon2 generator
 */
@CustomLog
public class FilenameGenerator {

    private static final String FILENAME_EXTENSION = "txt";
    private static final String FILENAME_FORMAT = "%s." + FILENAME_EXTENSION;

    private EncryptionService encryptionService;

    public FilenameGenerator() {
        this(new EncryptionService());
    }

    FilenameGenerator(EncryptionService encryptionService) {
        this.encryptionService = encryptionService;
    }

    /**
     * We will generate the file name by taking the cache value and encrypting it using the encryption key from the same cache value
     * @param cacheKey
     * @return
     */
    public String generate(CacheKey cacheKey) throws FilenameGenerationException {
        try {
            String filename = encryptionService.encrypt(cacheKey.getValue(), cacheKey.getEncryptionKey());
            return String.format(FILENAME_FORMAT, filename);
        } catch (EncryptionException e) {
            log.error("Failed to generate the filename since the encryption failed");
            throw new FilenameGenerationException();
        }
    }

}
