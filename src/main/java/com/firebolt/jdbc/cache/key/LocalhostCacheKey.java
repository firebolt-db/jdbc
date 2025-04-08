package com.firebolt.jdbc.cache.key;

import lombok.EqualsAndHashCode;

/**
 * When caching localhost connections, use the access token first 8 digits as the key
 */
@EqualsAndHashCode
public class LocalhostCacheKey implements CacheKey {

    private String value;

    public LocalhostCacheKey(String accessToken) {
        if (accessToken.length() > 8) {
            this.value = accessToken.substring(0, 8);
        } else {
            // if access token is less than 8 chars, append 8 hashes and then just take the first 8 chars
            value = new StringBuilder(accessToken).append("########").substring(0,8);
        }
    }

    @Override
    public String getValue() {
        return value;
    }
}
