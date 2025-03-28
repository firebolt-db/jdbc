package com.firebolt.jdbc.cache;

public enum CacheType {

    /**
     * Will only keep the cached values in memory
     */
    IN_MEMORY,

    /**
     * Will keep the cached values in memory while also writing them to disk.
     */
    ON_DISK;
}
