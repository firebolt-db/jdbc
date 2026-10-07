package com.firebolt.jdbc.cache;

import com.firebolt.jdbc.cache.exception.EncryptionException;
import com.firebolt.jdbc.cache.exception.FilenameGenerationException;
import com.firebolt.jdbc.cache.key.CacheKey;
import com.firebolt.jdbc.cache.key.ClientSecretCacheKey;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FilenameGeneratorTest {

    private static final String CACHE_KEY_VALUE = "key_value";
    private static final String CACHE_KEY_ENCRYPTION_KEY = "key to encrypt";

    private static final String FILENAME = "thefile";

    @Mock
    private EncryptionService mockEncryptionService;

    @Mock
    private CacheKey mockCacheKey;

    @InjectMocks
    private FilenameGenerator filenameGenerator;

    private void mockCacheKey() {
        when(mockCacheKey.getValue()).thenReturn(CACHE_KEY_VALUE);
        when(mockCacheKey.getEncryptionKey()).thenReturn(CACHE_KEY_ENCRYPTION_KEY);

        when(mockEncryptionService.encrypt(CACHE_KEY_VALUE, CACHE_KEY_ENCRYPTION_KEY)).thenReturn(FILENAME);
    }

    @Test
    void canGenerateFilename() throws Exception {
        mockCacheKey();
        byte[] hash = MessageDigest.getInstance("SHA-256").digest(FILENAME.getBytes(StandardCharsets.UTF_8));
        String expectedFileName = Base64.getUrlEncoder().withoutPadding().encodeToString(hash);
        assertEquals(expectedFileName + ".txt", filenameGenerator.generate(mockCacheKey));
    }

    @Test
    void filenameLengthDoesNotDependOnCredentialsLength() {
        FilenameGenerator generator = new FilenameGenerator();
        String longClientId = "i".repeat(500);
        String longSecret = "s".repeat(500);
        String longAccount = "a".repeat(500);

        String shortFilename = generator.generate(new ClientSecretCacheKey("id", "secret", "account"));
        String longFilename = generator.generate(new ClientSecretCacheKey(longClientId, longSecret, longAccount));

        assertEquals(shortFilename.length(), longFilename.length());
        // most file systems limit a file name to 255 bytes
        assertTrue(longFilename.length() < 255);
        assertTrue(longFilename.matches("[A-Za-z0-9_-]+\\.txt"));
    }

    @Test
    void differentCredentialsGenerateDifferentFilenames() {
        FilenameGenerator generator = new FilenameGenerator();
        assertNotEquals(generator.generate(new ClientSecretCacheKey("id", "secret", "account1")),
                generator.generate(new ClientSecretCacheKey("id", "secret", "account2")));
        assertEquals(generator.generate(new ClientSecretCacheKey("id", "secret", "account1")),
                generator.generate(new ClientSecretCacheKey("id", "secret", "account1")));
    }

    @Test
    void willNotGenerateFilenameWhenEncryptionFails() {
        mockCacheKey();
        when(mockEncryptionService.encrypt(CACHE_KEY_VALUE, CACHE_KEY_ENCRYPTION_KEY)).thenThrow(EncryptionException.class);
        assertThrows(FilenameGenerationException.class, () -> filenameGenerator.generate(mockCacheKey));
    }

}
