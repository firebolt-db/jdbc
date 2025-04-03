package com.firebolt.jdbc.cache;

import com.firebolt.jdbc.cache.exception.EncryptionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EncryptionServiceTest {

    private static final String VALID_ENCRYPTION_KEY = "valid encryption key";

    private EncryptionService encryptionService = new EncryptionService();

    @ParameterizedTest
    @ValueSource(strings = {
            "1234",
            "12345678",
            "1234567890123456", //16 chars
            "123456789012345678", //18 chars
            "12345678901234567890", //20 chars

    })
    void samePlainTextWithSameEncryptionKeyWouldEncryptToTheSameValue(String encryptionKey) {
        String firstEncryption = encryptionService.encrypt("text1", encryptionKey);
        String secondEncryption = encryptionService.encrypt("text1", encryptionKey);
        assertEquals(firstEncryption, secondEncryption);
    }

    @ParameterizedTest
    @CsvSource({
            "'plain text',''",
            "'','valid key'"
    })
    void cannotEncryptWithEmptyKeyOrPlainText(String text, String key) {
        assertThrows(IllegalArgumentException.class, () -> encryptionService.encrypt(text, key));
    }

    @Test
    void encryptionKeyHasToBeAtLeast16CharsLong() {
        assertNotNull(encryptionService.encrypt("text1", VALID_ENCRYPTION_KEY));
    }

    @Test
    void canDecryptTextEncryptedWithTheSameKey() {
        String encryptedText = encryptionService.encrypt("my text", "my key");
        assertEquals("my text", encryptionService.decrypt(encryptedText, "my key"));
    }

    @Test
    void cannotDecryptTextWithOtherKey() {
        String encryptedText = encryptionService.encrypt("my text", "my key");
        assertThrows(EncryptionException.class, () -> encryptionService.decrypt(encryptedText, "another key"));
    }

    @ParameterizedTest
    @CsvSource({
            "'decrypted key',''",
            "'','valid key'"
    })
    void cannotDecryptWithEmptyDecryptedTextOrEmptyKey(String encryptedText, String key) {
        assertThrows(IllegalArgumentException.class, () -> encryptionService.decrypt(encryptedText, key));
    }



}
