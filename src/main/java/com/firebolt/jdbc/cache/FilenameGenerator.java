package com.firebolt.jdbc.cache;

import com.firebolt.jdbc.annotation.ExcludeFromJacocoGeneratedReport;
import com.firebolt.jdbc.cache.exception.EncryptionException;
import com.firebolt.jdbc.cache.exception.FilenameGenerationException;
import com.firebolt.jdbc.cache.key.CacheKey;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import lombok.CustomLog;

/**
 * Generates a file name that should be unique for a cache key
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
     * We will generate the file name by taking the cache value and encrypting it using the encryption key from the same cache value.
     * The encrypted value grows with the length of the cache value, so it is hashed to keep the file name at a fixed length,
     * well below the 255 bytes limit most file systems have for a file name.
     * @param cacheKey
     * @return
     */
    public String generate(CacheKey cacheKey) throws FilenameGenerationException {
        try {
            String encryptedValue = encryptionService.encrypt(cacheKey.getValue(), cacheKey.getEncryptionKey());
            byte[] hash = MessageDigest.getInstance("SHA-256").digest(encryptedValue.getBytes(StandardCharsets.UTF_8));

            // url encoding, since the standard base64 alphabet has characters which are not valid in file names (e.g: / on linux and mac)
            String safeFilename = Base64.getUrlEncoder().withoutPadding().encodeToString(hash);

            return String.format(FILENAME_FORMAT, safeFilename);
        } catch (EncryptionException e) {
            log.error("Failed to generate the filename since the encryption failed");
            throw new FilenameGenerationException();
        } catch (NoSuchAlgorithmException e) {
            log.error("Failed to generate the filename since the hashing algorithm is not available");
            throw new FilenameGenerationException();
        }
    }

}
