package io.firebolt.jdbc.connection.settings;

import lombok.Builder;
import lombok.Value;

import java.util.Properties;

@Value
@Builder(toBuilder = true)
public class FireboltProperties {
  int timeToLiveMillis;
  int validateAfterInactivityMillis;
  int maxConnectionsPerRoute;
  int maxConnectionsTotal;
  int maxRetries;
  int bufferSize;
  int clientBufferSize;
  int socketTimeoutMillis;
  int connectionTimeoutMillis;
  int keepAliveTimeoutMillis;
  Integer port;
  String host;
  String database;
  String path;
  Boolean ssl;
  String sslCertificatePath;
  String sslMode;
  Integer compress;
  boolean decompress;
  Integer enableConnectionPool;
  String outputFormat;
  String user;
  String password;
  String engine;
  String account;
  Properties customProperties;
}
