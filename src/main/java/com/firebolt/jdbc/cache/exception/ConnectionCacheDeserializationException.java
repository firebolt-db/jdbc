package com.firebolt.jdbc.cache.exception;

/**
 * This error should be thrown when cannot deserialize the content of the disk file to a valid ConnectionCache.
 * This can happen because the file was tampered with.
 */
public class ConnectionCacheDeserializationException extends RuntimeException {
}
