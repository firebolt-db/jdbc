package com.firebolt.jdbc.cache;

import com.firebolt.jdbc.annotation.ExcludeFromJacocoGeneratedReport;
import com.firebolt.jdbc.cache.exception.EncryptionException;
import com.firebolt.jdbc.cache.exception.FilenameGenerationException;
import com.firebolt.jdbc.cache.key.CacheKey;
import java.util.Base64;
import lombok.CustomLog;

/**
 * Generates a file name that should be unique for a cache key using an argon2 generator
 */
@CustomLog
public class FilenameGenerator {

    private static final String FILENAME_EXTENSION = "txt";
    private static final String FILENAME_FORMAT = "%s." + FILENAME_EXTENSION;

    private EncryptionService encryptionService;

    @ExcludeFromJacocoGeneratedReport
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

            // the encryption returns a base64 string which have some characters which are not valid in file names (e.g: / on linux and mac). So encode the filename as url encoding
            String safeFilename = Base64.getUrlEncoder().withoutPadding().encodeToString(filename.getBytes());

            return String.format(FILENAME_FORMAT, safeFilename);
        } catch (EncryptionException e) {
            log.error("Failed to generate the filename since the encryption failed");
            throw new FilenameGenerationException();
        }
    }

}
