package com.firebolt.jdbc.cache;

import com.firebolt.jdbc.cache.exception.EncryptionException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
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
    private static final String SHA_256_ALGO = "SHA-256";

    /**
     * Encrypts a plaintext using the encryption key.
     *
     * @param plainText - the text to be encrypted
     * @param encryptionKey - the key to be used for encryption
     * @return - if encryption is successful then a base64 encoded encrypted string will be returned
     * @throws EncryptionException - when cannot encrypt the plain text
     */
    public String encrypt(String plainText, String encryptionKey) throws EncryptionException, IllegalArgumentException {
        if (StringUtils.isBlank(plainText) || StringUtils.isBlank(encryptionKey)) {
            throw new IllegalArgumentException("Text to encrypt or encryption key is null. Cannot encrypt.");
        }

        try {
            return encryptAESGCM(plainText, encryptionKey);
        } catch (Exception e) {
            log.error("Failed to encrypt the text", e);
            throw new EncryptionException("Encryption failed.");
        }
    }

    /**
     * Decrypts the encrypted text using AES algorithm
     * @param base64EncryptedText - the base64 encoded string
     * @param encryptionKey - the key to decrypt
     * @return the original plain text
     * @throws EncryptionException - in case the encrypted text cannot be decrypted
     */
    public String decrypt(String base64EncryptedText, String encryptionKey) throws EncryptionException, IllegalArgumentException {
        if (StringUtils.isBlank(base64EncryptedText) || StringUtils.isBlank(encryptionKey)) {
            throw new IllegalArgumentException("Text to decrypt or encryption key is null. Cannot decrypt.");
        }

        try {
            return decryptAESGCM(base64EncryptedText, encryptionKey);
        } catch (Exception e) {
            log.error("Failed to decrypt encrypted text", e);
            throw new EncryptionException("Decryption failed.");
        }
    }

    /**
     * Creates an encryption key with the correct length. AES algo uses 16, 24 or 32 bytes for the key. We will always use 32 bytes
     * @param input
     * @return
     * @throws Exception
     */
    private SecretKeySpec deriveAESKey(String input) throws NoSuchAlgorithmException {
        MessageDigest sha = MessageDigest.getInstance(SHA_256_ALGO);
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
    private String encryptAESGCM(String plaintext, String key) throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidAlgorithmParameterException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException {
        SecretKeySpec secretKey = deriveAESKey(key);

        // Generate a deterministic nonce from the key
        byte[] nonce = deriveNonce(key);
        GCMParameterSpec gcmSpec = new GCMParameterSpec(GCM_TAG_LENGTH, nonce);

        Cipher cipher = getAesGcmCipherInstance();
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
    private String decryptAESGCM(String encryptedBase64, String key) throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidAlgorithmParameterException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException {
        SecretKeySpec secretKey = deriveAESKey(key);

        // Regenerate the same deterministic nonce from key
        byte[] nonce = deriveNonce(key);
        GCMParameterSpec gcmSpec = new GCMParameterSpec(GCM_TAG_LENGTH, nonce);

        Cipher cipher = getAesGcmCipherInstance();
        cipher.init(Cipher.DECRYPT_MODE, secretKey, gcmSpec);

        byte[] encrypted = Base64.getDecoder().decode(encryptedBase64);
        byte[] decrypted = cipher.doFinal(encrypted);

        return new String(decrypted, StandardCharsets.UTF_8);
    }

    private Cipher getAesGcmCipherInstance() throws NoSuchPaddingException, NoSuchAlgorithmException {
        return Cipher.getInstance("AES/GCM/NoPadding");
    }

    // Deterministic nonce generation using SHA-256 hash
    private static byte[] deriveNonce(String key) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance(SHA_256_ALGO);
        byte[] hash = digest.digest((key + key).getBytes(StandardCharsets.UTF_8));
        byte[] nonce = new byte[12]; // AES-GCM nonce should be 12 bytes
        System.arraycopy(hash, 0, nonce, 0, nonce.length);
        return nonce;
    }
}
