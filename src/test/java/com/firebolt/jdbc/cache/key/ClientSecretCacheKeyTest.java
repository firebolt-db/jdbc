package com.firebolt.jdbc.cache.key;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ClientSecretCacheKeyTest {

    @Test
    void canGetEncryptionKey() {
        ClientSecretCacheKey clientSecretCacheKey = new ClientSecretCacheKey("some client id", "some client secret", "the account name");
        Assertions.assertEquals("some client secret", clientSecretCacheKey.getEncryptionKey());
    }
}
