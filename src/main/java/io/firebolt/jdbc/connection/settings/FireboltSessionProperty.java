package io.firebolt.jdbc.connection.settings;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public enum FireboltSessionProperty {
  PATH("path", "/", String.class, "Path component of the URI"),
  BUFFER_SIZE(
      "buffer_size", 65536, Integer.class, "The buffer used to create the ResultSet in bytes"),
  SSL("ssl", true, Boolean.class, "Enable SSL/TLS for the connection"),
  SSL_CERTIFICATE_PATH("ssl_certificate_path", "", String.class, "SSL/TLS root certificate"),
  SSL_MODE(
      "ssl_mode",
      "strict",
      String.class,
      "SSL mode to verify/not verify the certificate. Supported Types: none (don't verify), strict (verify)"),

  CLIENT_BUFFER_SIZE(
      "client_buffer_size",
      65536,
      Integer.class,
      "The buffer for the Apache client used by the Driver (in bytes). It is the preferred buffer size for the body of the http response. A larger buffer allows more content to be written before anything is actually sent while a smaller buffer decreases server memory load and allows the client to start receiving data quicker.\n"
          + "The buffer will be at least as large as the size requested."),
  MAX_RETRIES(
      "max_retries",
      3,
      Integer.class,
      "Maximum number of retries used by the client to query Firebolt. Set to 0 to disable"),

  SOCKET_TIMEOUT_MILLIS(
      "socket_timeout_millis",
      0,
      Integer.class,
      "Max time waiting for data after establishing a connection. A timeout value of zero is interpreted as an infinite timeout. A negative value is interpreted as undefined."),
  CONNECTION_TIMEOUT_MILLIS(
      "connection_timeout_millis",
      0,
      Integer.class,
      "Connection timeout in milliseconds. A timeout value of zero is interpreted as an infinite timeout"),
  KEEP_ALIVE_TIMEOUT_MILLIS(
      "connection_keep_alive_timeout_millis",
      Integer.MAX_VALUE,
      Integer.class,
      "How long a connection can remain idle before being reused (in milliseconds)."),
  TIME_TO_LIVE_MILLIS(
      "time_to_live_millis",
      60 * 1000,
      Integer.class,
      "Maximum life span of connections regardless of their connection_keep_alive_timeout_millis"),
  MAX_CONNECTIONS_PER_ROUTE(
      "max_connections_per_route", 500, Integer.class, "Maximum total connections per route"),
  MAX_CONNECTIONS_TOTAL("max_connections_total", 10000, Integer.class, "Maximum total connections"),

  ENABLE_CONNECTION_POOL(
      "enable_connection_pool", 0, Integer.class, "use connection pool for valid connections"),
  VALIDATE_AFTER_INACTIVITY_MILLIS(
      "validate_after_inactivity_millis",
      3 * 1000,
      Integer.class,
      "Defines period of inactivity in milliseconds after which persistent connections must be re-validated prior to being leased to the consumer. Non-positive value disables connection validation. "),
  COMPRESS(
      "compress",
      0,
      Integer.class,
      "Whether to compress transferred data or not. Compressed by default"),
  DECOMPRESS(
      "decompress",
      false,
      Boolean.class,
      "whether to decompress transferred data or not. Disabled by default"),
  DATABASE("database", null, String.class, "default database name"),
  PASSWORD("password", null, String.class, "user password - null by default"),
  USER("user", null, String.class, "user name - null by default"),
  HOST("host", null, String.class, "Firebolt host - null by default"),
  ENGINE("engine", null, String.class, "engine - null by default"),
  ACCOUNT("account", null, String.class, "account - null by default"),
  OUTPUT_FORMAT("output_format", null, String.class, "Format of the query results"),
  RESULT_OVERFLOW_MODE ("result_overflow_mode", null, String.class, "Action to do when the result exceed a limit. Throw -> Throw an exception, Break -> Same as LIMIT");

  private final String key;
  private final Object defaultValue;
  private final Class<?> clazz;
  private final String description;

  public String[] getPossibleValues() {
    return Boolean.class.equals(clazz) || Boolean.TYPE.equals(clazz)
        ? new String[] {"true", "false"}
        : null;
  }
}
