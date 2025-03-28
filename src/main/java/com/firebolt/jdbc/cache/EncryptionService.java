package com.firebolt.jdbc.cache;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Base64;
import java.util.Optional;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import lombok.CustomLog;
import org.apache.commons.lang3.StringUtils;

/**
 * Class that knows how to encrypt and encrypt secrets using AES encryption algorithm
 */
@CustomLog
public class EncryptionService {

    private static final int GCM_TAG_LENGTH = 128; // GCM tag length in bits (16 bytes)
    private static final int AES_KEY_SIZE = 32; // use 32 bytes so we will use AES-256 algorithm

    /**
     * Encrypts a plaintext using the encryption key. If there are any exceptions during encryption an empty optional will be returned
     *
     * @param plainText - the text to be encrypted
     * @param encryptionKey - the key to be used for encryption
     * @return - if encryption is successful then a base64 encoded encrypted string will be returned
     */
    public Optional<String> encrypt(String plainText, String encryptionKey) {
        if (StringUtils.isBlank(plainText) || StringUtils.isBlank(encryptionKey)) {
            return Optional.empty();
        }

        try {
            return Optional.of(encryptAESGCM(plainText, encryptionKey));
        } catch (Exception e) {
            log.error("Failed to encrypt the text", e);
            return Optional.empty();
        }
    }

    /**
     * Decrypts the encrypted text using AES algorithm
     * @param base64EncryptedText - the base64 encoded string
     * @param encryptionKey - the key to decrypt
     * @return the original plain text
     * @throws Exception - in case the encrypted text cannot be decrypted
     */
    public Optional<String> decrypt(String base64EncryptedText, String encryptionKey) {
        if (StringUtils.isBlank(base64EncryptedText) || StringUtils.isBlank(encryptionKey)) {
            return Optional.empty();
        }

        try {
            return Optional.of(decryptAESGCM(base64EncryptedText, encryptionKey));
        } catch (Exception e) {
            log.error("Failed to decrypt encrypted text", e);
            return Optional.empty();
        }
    }

    /**
     * Creates an encryption key with the correct length. AES algo uses 16, 24 or 32 bytes for the key. We will always use 32 bytes
     * @param input
     * @return
     * @throws Exception
     */
    private SecretKeySpec deriveAESKey(String input) throws Exception {
        MessageDigest sha = MessageDigest.getInstance("SHA-256");
        byte[] hash = sha.digest(input.getBytes(StandardCharsets.UTF_8));
        return new SecretKeySpec(Arrays.copyOf(hash, AES_KEY_SIZE), "AES"); // Trim to required length
    }

    /**
     * Return the base64 encoded encrypted text
     * @param plaintext - the text to be encrypted
     * @param key - the key to use for encryption
     * @return
     * @throws Exception - exception when cannot encrypt the plaintext
     */
    private String encryptAESGCM(String plaintext, String key) throws Exception {
        SecretKeySpec secretKey = deriveAESKey(key);

        // Generate a deterministic nonce from the key
        byte[] nonce = deriveNonce(key, key);
        GCMParameterSpec gcmSpec = new GCMParameterSpec(GCM_TAG_LENGTH, nonce);

        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.ENCRYPT_MODE, secretKey, gcmSpec);
        byte[] encrypted = cipher.doFinal(plaintext.getBytes(StandardCharsets.UTF_8));

        return Base64.getEncoder().encodeToString(encrypted);
    }

    /**
     * Decrypts the encrypted text using AES algorithm
     * @param encryptedBase64 - the base64 encoded string
     * @param key - the key to decrypt
     * @return the original plain text
     * @throws Exception - in case the encrypted text cannot be decrypted
     */
    private String decryptAESGCM(String encryptedBase64, String key) throws Exception {
        SecretKeySpec secretKey = deriveAESKey(key);

        // Regenerate the same deterministic nonce from key
        byte[] nonce = deriveNonce(key, key);
        GCMParameterSpec gcmSpec = new GCMParameterSpec(GCM_TAG_LENGTH, nonce);

        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.DECRYPT_MODE, secretKey, gcmSpec);

        byte[] encrypted = Base64.getDecoder().decode(encryptedBase64);
        byte[] decrypted = cipher.doFinal(encrypted);

        return new String(decrypted, StandardCharsets.UTF_8);
    }

    // Deterministic nonce generation using SHA-256 hash
    private static byte[] deriveNonce(String plaintext, String key) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest((plaintext + key).getBytes(StandardCharsets.UTF_8));
        byte[] nonce = new byte[12]; // AES-GCM nonce should be 12 bytes
        System.arraycopy(hash, 0, nonce, 0, nonce.length);
        return nonce;
    }
}
