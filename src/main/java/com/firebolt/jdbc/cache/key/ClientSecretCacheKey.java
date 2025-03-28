package com.firebolt.jdbc.cache.key;

import lombok.CustomLog;
import lombok.EqualsAndHashCode;

@CustomLog
@EqualsAndHashCode
public class ClientSecretCacheKey implements CacheKey {

    private String value;

    public ClientSecretCacheKey(String clientId, String clientSecret, String accountName) {
        value = hashValues(clientId, clientSecret, accountName);
    }

    @Override
    public String getValue() {
        return value;
    }

    private String hashValues(String clientId, String clientSecret, String accountName) {
        return String.join("#", clientId, clientSecret, accountName);
    }
}
