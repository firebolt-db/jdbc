package io.firebolt.jdbc.connection.settings;

import lombok.Builder;
import lombok.Value;

import java.util.Properties;

@Value
@Builder(toBuilder = true)
public class FireboltProperties {
    int timeToLiveMillis;
    int validateAfterInactivityMillis;
    int defaultMaxPerRoute;
    int maxTotal;
    int maxRetries;
    int bufferSize;
    int apacheBufferSize;
    int socketTimeout;
    int connectionTimeout;
    int keepAliveTimeout;
    Integer port;
    String host;
    Boolean usePathAsDb;
    String database;
    String path;
    Boolean ssl;
    String sslRootCertificate;
    String sslMode;
    Integer compress;
    boolean decompress;
    Integer useConnectionPool;
    String outputFormat;
    String user;
    String password;
    String engine;
    String account;
    Properties customProperties;
}
