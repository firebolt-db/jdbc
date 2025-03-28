package com.firebolt.jdbc.cache;

import com.firebolt.jdbc.cache.key.CacheKey;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class FilenameGeneratorTest {

    private static String CACHE_KEY_VALUE = "key_value";
    private static String CACHE_KEY_ENCRYPTION_KEY = "key to encrypt";

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

        when(mockEncryptionService.encrypt(CACHE_KEY_VALUE, CACHE_KEY_ENCRYPTION_KEY)).thenReturn(Optional.of(FILENAME));
    }

    @Test
    void canGenerateFilename() {
        assertEquals(FILENAME + ".txt", filenameGenerator.generate(mockCacheKey).get());
    }


    @Test
    void willNotGenerateFilenameWhenEncryptionFails() {
        when(mockEncryptionService.encrypt(CACHE_KEY_VALUE, CACHE_KEY_ENCRYPTION_KEY)).thenReturn(Optional.empty());
        assertTrue(filenameGenerator.generate(mockCacheKey).isEmpty());
    }

}
