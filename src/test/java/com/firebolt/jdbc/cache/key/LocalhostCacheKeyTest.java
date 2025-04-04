package com.firebolt.jdbc.cache.key;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LocalhostCacheKeyTest {

    @Test
    void canGetEncryptionKey() {
        LocalhostCacheKey localhostCacheKey = new LocalhostCacheKey("the access token");
        assertEquals("very_strong_encryption_key", localhostCacheKey.getEncryptionKey());
    }
}
