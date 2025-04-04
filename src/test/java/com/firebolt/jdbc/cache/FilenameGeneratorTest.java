package com.firebolt.jdbc.cache;

import com.firebolt.jdbc.cache.exception.EncryptionException;
import com.firebolt.jdbc.cache.exception.FilenameGenerationException;
import com.firebolt.jdbc.cache.key.CacheKey;
import java.util.Base64;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

    @BeforeEach
    void init() {
        when(mockCacheKey.getValue()).thenReturn(CACHE_KEY_VALUE);
        when(mockCacheKey.getEncryptionKey()).thenReturn(CACHE_KEY_ENCRYPTION_KEY);

        when(mockEncryptionService.encrypt(CACHE_KEY_VALUE, CACHE_KEY_ENCRYPTION_KEY)).thenReturn(FILENAME);
    }

    @Test
    void canGenerateFilename() {
        String expectedFileName = Base64.getUrlEncoder().withoutPadding().encodeToString(FILENAME.getBytes());
        assertEquals(expectedFileName + ".txt", filenameGenerator.generate(mockCacheKey));
    }

    @Test
    void willNotGenerateFilenameWhenEncryptionFails() {
        when(mockEncryptionService.encrypt(CACHE_KEY_VALUE, CACHE_KEY_ENCRYPTION_KEY)).thenThrow(EncryptionException.class);
        assertThrows(FilenameGenerationException.class, () -> filenameGenerator.generate(mockCacheKey));
    }

}
