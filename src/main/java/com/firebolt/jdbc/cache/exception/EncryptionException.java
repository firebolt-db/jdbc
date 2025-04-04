package com.firebolt.jdbc.cache.exception;

public class EncryptionException extends RuntimeException {

    public static EncryptionException encryptionFailed() {
        return new EncryptionException();
    }

    public static EncryptionException decryptionFailed() {
        return new EncryptionException();
    }
}
