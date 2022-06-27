package io.firebolt.jdbc.connection.settings;

import lombok.Builder;
import lombok.Value;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Value
@Builder(toBuilder = true)
public class FireboltProperties {

  private static final Pattern DB_PATH_PATTERN = Pattern.compile("/([a-zA-Z0-9_*\\-]+)");
  private static final int FIREBOLT_SSL_PROXY_PORT = 443;
  private static final int FIREBOLT_NO_SSL_PROXY_PORT = 9090;

  private static final Set<String> sessionPropertyKeys =
      Arrays.stream(FireboltSessionProperty.values())
          .map(FireboltSessionProperty::getKey)
          .collect(Collectors.toSet());

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
  @Builder.Default Map<String, String> additionalProperties = new HashMap<>();

  public static FireboltProperties of(Properties... properties) {
    Properties mergedProperties = mergeProperties(properties);
    boolean ssl = getSetting(mergedProperties, FireboltSessionProperty.SSL);
    String sslRootCertificate =
        getSetting(mergedProperties, FireboltSessionProperty.SSL_CERTIFICATE_PATH);
    String sslMode = getSetting(mergedProperties, FireboltSessionProperty.SSL_MODE);
    Integer compress = getSetting(mergedProperties, FireboltSessionProperty.COMPRESS);
    boolean decompress = getSetting(mergedProperties, FireboltSessionProperty.DECOMPRESS);
    Integer useConnectionPool =
        getSetting(mergedProperties, FireboltSessionProperty.ENABLE_CONNECTION_POOL);
    String user = getSetting(mergedProperties, FireboltSessionProperty.USER);
    String password = getSetting(mergedProperties, FireboltSessionProperty.PASSWORD);
    String path = getSetting(mergedProperties, FireboltSessionProperty.PATH);
    String engine = getSetting(mergedProperties, FireboltSessionProperty.ENGINE);
    String account = getSetting(mergedProperties, FireboltSessionProperty.ACCOUNT);
    int maxConnectionsPerRoute =
        getSetting(mergedProperties, FireboltSessionProperty.MAX_CONNECTIONS_PER_ROUTE);
    int timeToLiveMillis =
        getSetting(mergedProperties, FireboltSessionProperty.TIME_TO_LIVE_MILLIS);
    int validateAfterInactivityMillis =
        getSetting(mergedProperties, FireboltSessionProperty.VALIDATE_AFTER_INACTIVITY_MILLIS);
    int maxTotal = getSetting(mergedProperties, FireboltSessionProperty.MAX_CONNECTIONS_TOTAL);
    int maxRetries = getSetting(mergedProperties, FireboltSessionProperty.MAX_RETRIES);
    int bufferSize = getSetting(mergedProperties, FireboltSessionProperty.BUFFER_SIZE);
    int clientBufferSize = getSetting(mergedProperties, FireboltSessionProperty.CLIENT_BUFFER_SIZE);
    int socketTimeout = getSetting(mergedProperties, FireboltSessionProperty.SOCKET_TIMEOUT_MILLIS);
    int connectionTimeout =
        getSetting(mergedProperties, FireboltSessionProperty.CONNECTION_TIMEOUT_MILLIS);
    int keepAliveTimeout =
        getSetting(mergedProperties, FireboltSessionProperty.KEEP_ALIVE_TIMEOUT_MILLIS);
    String host = getHost(mergedProperties);
    Integer port = getPort(ssl);
    String database = getDatabase(mergedProperties, path);
    Map<String, String> additionalProperties = getFireboltCustomProperties(mergedProperties);

    return FireboltProperties.builder()
        .ssl(ssl)
        .sslCertificatePath(sslRootCertificate)
        .sslMode(sslMode)
        .path(path)
        .port(port)
        .database(database)
        .compress(compress)
        .decompress(decompress)
        .enableConnectionPool(useConnectionPool)
        .user(user)
        .password(password)
        .host(host)
        .ssl(ssl)
        .additionalProperties(additionalProperties)
        .account(account)
        .engine(engine)
        .maxConnectionsPerRoute(maxConnectionsPerRoute)
        .timeToLiveMillis(timeToLiveMillis)
        .validateAfterInactivityMillis(validateAfterInactivityMillis)
        .maxConnectionsTotal(maxTotal)
        .maxRetries(maxRetries)
        .clientBufferSize(clientBufferSize)
        .bufferSize(bufferSize)
        .socketTimeoutMillis(socketTimeout)
        .connectionTimeoutMillis(connectionTimeout)
        .keepAliveTimeoutMillis(keepAliveTimeout)
        .build();
  }

  private static String getHost(Properties properties) {
    String host = getSetting(properties, FireboltSessionProperty.HOST);
    if (StringUtils.isEmpty(host)) {
      throw new IllegalArgumentException("Invalid host: The host is missing or empty");
    } else {
      return host;
    }
  }

  @NotNull
  private static Integer getPort(boolean ssl) {
    return ssl ? FIREBOLT_SSL_PROXY_PORT : FIREBOLT_NO_SSL_PROXY_PORT;
  }

  private static String getDatabase(Properties properties, String path)
      throws IllegalArgumentException {
    String database = getSetting(properties, FireboltSessionProperty.DATABASE);
    if (StringUtils.isEmpty(database)) {
      if ("/".equals(path)) {
        throw new IllegalArgumentException("A database must be provided");
      } else {
        Matcher m = DB_PATH_PATTERN.matcher(path);
        if (m.matches()) {
          return m.group(1);
        } else {
          throw new IllegalArgumentException(
              String.format("The database provided is invalid %s", path));
        }
      }
    } else {
      return database;
    }
  }

  private static Map<String, String> getFireboltCustomProperties(Properties properties) {
    return properties.entrySet().stream()
        .filter(entry -> !sessionPropertyKeys.contains(entry.getKey()))
        .collect(
            Collectors.toMap(e -> (String) e.getKey(), e -> e.getValue().toString(), (x, y) -> y));
  }

  private static <T> T getSetting(Properties info, FireboltSessionProperty param) {
    return getSetting(info, param.getKey(), param.getDefaultValue(), param.getClazz());
  }

  @SuppressWarnings("unchecked")
  private static <T> T getSetting(
      Properties info, String key, Object defaultValue, Class<?> clazz) {
    String val = info.getProperty(key);
    if (val == null) return (T) defaultValue;
    if (clazz == int.class || clazz == Integer.class) {
      return (T) clazz.cast(Integer.valueOf(val));
    }
    if (clazz == long.class || clazz == Long.class) {
      return (T) clazz.cast(Long.valueOf(val));
    }
    if (clazz == boolean.class || clazz == Boolean.class) {
      final Boolean boolValue =
          "1".equals(val) || "0".equals(val) ? "1".equals(val) : Boolean.parseBoolean(val);
      return (T) clazz.cast(boolValue);
    }
    return (T) clazz.cast(val);
  }

  private static Properties mergeProperties(Properties... properties) {
    Properties mergedProperties = new Properties();
    for (Properties p : properties) {
      mergedProperties.putAll(p);
    }
    return mergedProperties;
  }

  public void addProperty(String key, String value) {
    additionalProperties.put(key, value);
  }

  public void addProperty(Pair<String, String> property) {
    this.addProperty(property.getLeft(), property.getRight());
  }

  public static FireboltProperties copy(FireboltProperties properties) {
    return properties.toBuilder()
        .additionalProperties(new HashMap<>(properties.getAdditionalProperties()))
        .build();
  }
}
