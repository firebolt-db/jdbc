package com.firebolt.jdbc.cache.key;

import lombok.CustomLog;
import lombok.EqualsAndHashCode;

@CustomLog
@EqualsAndHashCode
public class ClientSecretCacheKey implements CacheKey {

    private String value;
    private String clientSecret;

    public ClientSecretCacheKey(String clientId, String clientSecret, String accountName) {
        value = hashValues(clientId, clientSecret, accountName);
        this.clientSecret = clientSecret;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public String getEncryptionKey() {
        return clientSecret;
    }

    private String hashValues(String clientId, String clientSecret, String accountName) {
        return String.join("#", clientId, clientSecret, accountName);
    }
}
