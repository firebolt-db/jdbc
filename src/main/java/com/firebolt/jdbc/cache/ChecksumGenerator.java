package com.firebolt.jdbc.cache;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Optional;
import lombok.CustomLog;

/**
 * Generates a checksum for a ConnectionCache
 */
@CustomLog
public class ChecksumGenerator {

    /**
     * Will generate a checksum from the attributes of the object. Only these attributes are used in calculating the checksum:
     *  - connectionId
     *  - accessToken
     *  - systemEngineUrl
     *  - databaseOptionsMap
     *  - engineOptionsMap
     *
     * @return
     */
    public Optional<String> generateChecksum(ConnectionCache connectionCache) {
        if (connectionCache == null) {
            log.error("Cannot generate checksum for null connection cache");
            return Optional.empty();
        }

        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            log.error("Did not find the sha-256 algorithm so cannot create the checksum");
            return Optional.empty();
        }

        String connectionCacheAsString = connectionCache.asChecksumString();
        return Optional.ofNullable(checksum(digest, connectionCacheAsString));
    }

    private String checksum(MessageDigest messageDigest, String checksumValue) {
        byte[] hashBytes = messageDigest.digest(checksumValue.getBytes());

        // Convert bytes to Hex string
        StringBuilder hexString = new StringBuilder();
        for (byte b : hashBytes) {
            hexString.append(String.format("%02x", b));
        }
        return hexString.toString();

    }
}
