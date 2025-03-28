package com.firebolt.jdbc.cache;

import com.firebolt.jdbc.cache.key.CacheKey;
import java.util.Optional;

/**
 * Generates a file name that should be unique for a cache key using an argon2 generator
 */
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
    public Optional<String> generate(CacheKey cacheKey) {
        return encryptionService.encrypt(cacheKey.getValue(), cacheKey.getEncryptionKey()).map(name -> String.format(FILENAME_FORMAT, name));
    }

}
