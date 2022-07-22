package com.firebolt.jdbc.connection.settings;

import lombok.Getter;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Getter
public enum FireboltSessionProperty {
  PATH("path", "/", String.class, false, "Path component of the URI"),
  BUFFER_SIZE(
      "buffer_size",
      65536,
      Integer.class,
      false,
      "The buffer used to create the ResultSet in bytes"),
  SSL("ssl", true, Boolean.class, false, "Enable SSL/TLS for the connection"),
  SSL_CERTIFICATE_PATH(
      "ssl_certificate_path", "", String.class, false, "SSL/TLS root certificate", "sslrootcert"),
  SSL_MODE(
      "ssl_mode",
      "strict",
      String.class,
      false,
      "SSL mode to verify/not verify the certificate. Supported Types: none (don't verify), strict (verify)",
      "sslmode"),

  CLIENT_BUFFER_SIZE(
      "client_buffer_size",
      65536,
      Integer.class,
      false,
      "The buffer for the Apache client used by the Driver (in bytes). It is the preferred buffer size for the body of the http response. A larger buffer allows more content to be written before anything is actually sent while a smaller buffer decreases server memory load and allows the client to start receiving data quicker.\n"
          + "The buffer will be at least as large as the size requested.",
      "apache_buffer_size"),
  MAX_RETRIES(
      "max_retries",
      3,
      Integer.class,
      false,
      "Maximum number of retries used by the client to query Firebolt. Set to 0 to disable"),

  SOCKET_TIMEOUT_MILLIS(
      "socket_timeout_millis",
      0,
      Integer.class,
      false,
      "Max time waiting for data after establishing a connection. A timeout value of zero is interpreted as an infinite timeout. A negative value is interpreted as undefined.",
      "socket_timeout"),
  CONNECTION_TIMEOUT_MILLIS(
      "connection_timeout_millis",
      0,
      Integer.class,
      false,
      "Connection timeout in milliseconds. A timeout value of zero is interpreted as an infinite timeout",
      "connection_timeout"),
  KEEP_ALIVE_TIMEOUT_MILLIS(
      "connection_keep_alive_timeout_millis",
      Integer.MAX_VALUE,
      Integer.class,
      false,
      "How long a connection can remain idle before being reused (in milliseconds).",
      "keepAliveTimeout"),
  TIME_TO_LIVE_MILLIS(
      "time_to_live_millis",
      60 * 1000,
      Integer.class,
      false,
      "Maximum life span of connections regardless of their connection_keep_alive_timeout_millis",
      "timeToLiveMillis"),
  MAX_CONNECTIONS_PER_ROUTE(
      "max_connections_per_route",
      500,
      Integer.class,
      false,
      "Maximum total connections per route",
      "defaultMaxPerRoute"),
  MAX_CONNECTIONS_TOTAL(
      "max_connections_total",
      10000,
      Integer.class,
      false,
      "Maximum total connections",
      "maxTotal"),

  USE_CONNECTION_POOL( // Added it for backward compatibility but not used
      "use_connection_pool",
      false,
      Boolean.class,
      false,
      "use connection pool for valid connections. This property is deprecated and setting it has no effect."),
  VALIDATE_AFTER_INACTIVITY_MILLIS(
      "validate_after_inactivity_millis",
      3 * 1000,
      Integer.class,
      false,
      "Defines period of inactivity in milliseconds after which persistent connections must be re-validated prior to being leased to the consumer. Non-positive value disables connection validation. "),

  TCP_KEEP_IDLE(
      "tcp_keep_idle",
      60,
      Integer.class,
      false,
      "TCP option that defines the number of seconds of idle time before keep-alive initiates a probe. TCP probes a connection that has been idle for some amount of time. If the remote system does not respond to a keep-alive probe, TCP retransmits the probe after some amount of time."),
  TCP_KEEP_COUNT(
      "tcp_keep_count",
      10,
      Integer.class,
      false,
      "TCP option that defines the maximum number of keep-alive probes to be sent. TCP probes a connection that has been idle for some amount of time. If the remote system does not respond to a keep-alive probe, TCP retransmits the probe a certain number of times before a connection is considered to be broken."),
  TCP_KEEP_INTERVAL(
      "tcp_keep_interval",
      30,
      Integer.class,
      false,
      "TCP option that defines the number of seconds to wait before retransmitting a keep-alive probe. TCP probes a connection that has been idle for some amount of time. If the remote system does not respond to a keep-alive probe, TCP retransmits the probe after some amount of time."),
  COMPRESS( // compress should always be used as the HTTP response code is sometimes incorrect when
      // not using it
      "compress",
      true,
      Boolean.class,
      false,
      "Whether to compress transferred data or not. Compressed by default"),
  DATABASE("database", null, String.class, false, "default database name"),
  PASSWORD("password", null, String.class, false, "user password - null by default"),
  USER("user", null, String.class, false, "user name - null by default"),
  HOST("host", null, String.class, false, "Firebolt host - null by default"),
  PORT("port", null, Integer.class, false, "Firebolt port - null by default"),
  ENGINE("engine", null, String.class, false, "engine - null by default"),
  AGGRESSIVE_CANCEL(
      "aggressive_cancel",
      false,
      Boolean.class,
      false,
      "enable aggressive cancel. Permits to cancel queries by sending a query to Firebolt rather than calling the /cancel endpoint"),
  ACCOUNT("account", null, String.class, false, "account - null by default"),

  // Added for backward compatibility but it is not used anymore
  USE_PATH_AS_DB(
      "use_path_as_db",
      null,
      Boolean.class,
      true,
      "When set to true (the default) or not specified, the path parameter from the URL is used as the database name"),
  LOG_RESULT_SET(
      "log_result_set",
      false,
      Boolean.class,
      false,
      "When set to true, the result of the queries executed are logged with the log level INFO. This has a negative performance impact and should only be enabled for debugging purposes");

  private final String key;
  private final Object defaultValue;
  private final Class<?> clazz;
  private final String description;
  private final String[] aliases;

  private final boolean deprecated;

  FireboltSessionProperty(
      String key,
      Object defaultValue,
      Class<?> clazz,
      boolean deprecated,
      String description,
      String... aliases) {
    this.key = key;
    this.defaultValue = defaultValue;
    this.clazz = clazz;
    this.deprecated = deprecated;
    this.description = description;
    this.aliases = aliases != null ? aliases : new String[] {};
  }

  public String[] getPossibleValues() {
    return Boolean.class.equals(clazz) || Boolean.TYPE.equals(clazz)
        ? new String[] {"true", "false"}
        : null;
  }

  public static List<FireboltSessionProperty> getNonDeprecatedProperties() {
    return Arrays.stream(FireboltSessionProperty.values())
        .filter(property -> !property.deprecated)
        .collect(Collectors.toList());
  }

  public static Optional<FireboltSessionProperty> of(String key) {
    return Arrays.stream(values()).filter(v -> v.key.equals(key)).findAny();
  }
}
