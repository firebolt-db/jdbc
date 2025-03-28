package com.firebolt.jdbc.cache;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class EncryptionServiceTest {

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
        String firstEncryption = encryptionService.encrypt("text1", encryptionKey).get();
        String secondEncryption = encryptionService.encrypt("text1", encryptionKey).get();
        assertEquals(firstEncryption, secondEncryption);
    }

    @Test
    void encryptionKeyHasToBeAtLeast16CharsLong() {
        assertTrue(encryptionService.encrypt("text1", "").isEmpty());
    }

    @Test
    void canDecryptTextEncryptedWithTheSameKey() {
        String encryptedText = encryptionService.encrypt("my text", "my key").get();
        assertEquals("my text", encryptionService.decrypt(encryptedText, "my key").get());
    }

    @Test
    void cannotDecryptTextWithOtherKey() {
        String encryptedText = encryptionService.encrypt("my text", "my key").get();
        assertTrue(encryptionService.decrypt(encryptedText, "another key").isEmpty());
    }


}
