package io.firebolt.jdbc.connection.settings;


import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * session properties that can be used to open a session
 */
@RequiredArgsConstructor
@Getter
public enum FireboltSessionProperty {

    BUFFER_SIZE("bufferSize", 65536, Integer.class, "The buffer for the response in bytes (Buffer size of the ResultSet returned by the driver)"),
    APACHE_BUFFER_SIZE("apacheBufferSize", 65536, Integer.class, "The buffer for the Apache client used by the Driver (in bytes). It is the preferred buffer size for the body of the http response. A larger buffer allows more content to be written before anything is actually sent while a smaller buffer decreases server memory load and allows the client to start receiving data quicker.\n" +
            "The buffer will be at least as large as the size requested."),
    SOCKET_TIMEOUT("socketTimeout", Integer.MAX_VALUE, Integer.class, "Max time waiting for data after establishing a connection. A timeout value of zero is interpreted as an infinite timeout. A negative value is interpreted as undefined."),
    CONNECTION_TIMEOUT("connectionTimeout", Integer.MAX_VALUE, Integer.class, "Connection timeout in milliseconds. A timeout value of zero is interpreted as an infinite timeout"),
    SSL("ssl", true, Boolean.class, "Enable SSL/TLS for the connection"),
    SSL_ROOT_CERTIFICATE("sslRootCert", "", String.class, "SSL/TLS root certificate"),
    SSL_MODE("sslMode", "strict", String.class, "SSL mode to verify/not verify the certificate. Supported Types: none (don't verify), strict (verify)"),
    USE_PATH_AS_DB("usePathAsDb", true, Boolean.class, "Enable/disable the usage of the URL path as the database."),
    PATH("path", "/", String.class, "URL path"),
    CHECK_FOR_REDIRECTS("checkForRedirects", false, Boolean.class, "Whether 307 redirects should be checked using GET before sending POST to given URL"),
    MAX_REDIRECTS("maxRedirects", 5, Integer.class, "Maximum of redirects before using the last URL"),
    MAX_RETRIES("maxRetries", 3, Integer.class, "Maximum number of retries. Set to 0 to disable"),
    KEEP_ALIVE_TIMEOUT("keepAliveTimeout", Integer.MAX_VALUE, Integer.class, "How long a connection can remain idle before being reused (in milliseconds)."),
    TIME_TO_LIVE_MILLIS("timeToLiveMillis", 60 * 1000, Integer.class, "Maximum life span of connections regardless of their keepAliveTimeout"),
    DEFAULT_MAX_PER_ROUTE("defaultMaxPerRoute", 500, Integer.class, "Maximum total connections per route"),
    MAX_TOTAL("maxTotal", 10000, Integer.class, "Maximum total connections"),
    VALIDATE_AFTER_INACTIVITY_MILLIS("validateAfterInactivityMillis", 3 * 1000, Integer.class, "Defines period of inactivity in milliseconds after which persistent connections must be re-validated prior to being leased to the consumer. Non-positive value passed to this method disables connection validation. "),
    USE_OBJECTS_IN_ARRAYS("useObjectsInArrays", false, Boolean.class, "Whether arrays of objects (Object[]) should be used instead of primitive arrays. False by default"),
    USE_CONNECTION_POOL("useConnectionPool", 0, Integer.class, "use connection pool for valid connections"),
    COMPRESS("compress", 1, Integer.class, "Whether to compress transferred data or not. Compressed by default"),
    DECOMPRESS("decompress", false, Boolean.class, "whether to decompress transferred data or not. Disabled by default"),
    DATABASE("database", "default", String.class, "default database name"),
    PASSWORD("password", null, String.class, "user password - null by default"),
    USER("user", null, String.class, "user name - null by default"),
    HOST("host", null, String.class, "Firebolt host - null by default"),
    PORT("port", null, Integer.class, "port - null by default"),
    ENGINE("engine", null, String.class, "engine - null by default"),
    ACCOUNT("account", null, String.class, "account - null by default"),
    OUTPUT_FORMAT("outputFormat", null, String.class, "Format of the query results");

    private final String key;
    private final Object defaultValue;
    private final Class<?> clazz;
    private final String description;
}
