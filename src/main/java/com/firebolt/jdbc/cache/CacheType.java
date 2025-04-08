package com.firebolt.jdbc.cache;

public enum CacheType {

    /**
     * Will only keep the cached values in memory
     */
    MEMORY,

    /**
     * Will keep the cached values in memory while also writing them to disk.
     */
    DISK;
}
