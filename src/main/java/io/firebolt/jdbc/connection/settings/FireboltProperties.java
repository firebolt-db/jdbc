package io.firebolt.jdbc.connection.settings;

import lombok.Builder;
import lombok.Value;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
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
  String defaultTimeZone;
  @Builder.Default List<Pair<String, String>> additionalProperties = new ArrayList<>();

  public static FireboltProperties of(Properties... properties) {
    Properties mergedProperties = mergeProperties(properties);
    boolean ssl = getSetting(mergedProperties, FireboltSessionProperty.SSL);
    String sslRootCertificate =
        getSetting(mergedProperties, FireboltSessionProperty.SSL_ROOT_CERTIFICATE);
    String sslMode = getSetting(mergedProperties, FireboltSessionProperty.SSL_MODE);
    boolean usePathAsDb = getSetting(mergedProperties, FireboltSessionProperty.USE_PATH_AS_DB);
    Integer compress = getSetting(mergedProperties, FireboltSessionProperty.COMPRESS);
    boolean decompress = getSetting(mergedProperties, FireboltSessionProperty.DECOMPRESS);
    Integer useConnectionPool =
        getSetting(mergedProperties, FireboltSessionProperty.USE_CONNECTION_POOL);
    String user = getSetting(mergedProperties, FireboltSessionProperty.USER);
    String password = getSetting(mergedProperties, FireboltSessionProperty.PASSWORD);
    String path = getSetting(mergedProperties, FireboltSessionProperty.PATH);
    String engine = getSetting(mergedProperties, FireboltSessionProperty.ENGINE);
    String account = getSetting(mergedProperties, FireboltSessionProperty.ACCOUNT);
    int defaultMaxPerRoute =
        getSetting(mergedProperties, FireboltSessionProperty.DEFAULT_MAX_PER_ROUTE);
    int timeToLiveMillis =
        getSetting(mergedProperties, FireboltSessionProperty.TIME_TO_LIVE_MILLIS);
    int validateAfterInactivityMillis =
        getSetting(mergedProperties, FireboltSessionProperty.VALIDATE_AFTER_INACTIVITY_MILLIS);
    int maxTotal = getSetting(mergedProperties, FireboltSessionProperty.MAX_TOTAL);
    int maxRetries = getSetting(mergedProperties, FireboltSessionProperty.MAX_RETRIES);
    int bufferSize = getSetting(mergedProperties, FireboltSessionProperty.BUFFER_SIZE);
    int apacheBufferSize = getSetting(mergedProperties, FireboltSessionProperty.APACHE_BUFFER_SIZE);
    String outputFormat = getSetting(mergedProperties, FireboltSessionProperty.OUTPUT_FORMAT);
    int socketTimeout = getSetting(mergedProperties, FireboltSessionProperty.SOCKET_TIMEOUT);
    int connectionTimeout =
        getSetting(mergedProperties, FireboltSessionProperty.CONNECTION_TIMEOUT);
    int keepAliveTimeout = getSetting(mergedProperties, FireboltSessionProperty.KEEP_ALIVE_TIMEOUT);
    String host = getHost(mergedProperties);
    Integer port = getPort(mergedProperties, ssl);
    String database = getDatabase(mergedProperties, usePathAsDb, path);
    List<Pair<String, String>> additionalProperties = getFireboltCustomProperties(mergedProperties);

    return FireboltProperties.builder()
        .ssl(ssl)
        .sslRootCertificate(sslRootCertificate)
        .sslMode(sslMode)
        .usePathAsDb(usePathAsDb)
        .path(path)
        .port(port)
        .database(database)
        .compress(compress)
        .decompress(decompress)
        .useConnectionPool(useConnectionPool)
        .user(user)
        .password(password)
        .host(host)
        .ssl(ssl)
        .account(account)
        .engine(engine)
        .defaultMaxPerRoute(defaultMaxPerRoute)
        .timeToLiveMillis(timeToLiveMillis)
        .validateAfterInactivityMillis(validateAfterInactivityMillis)
        .maxTotal(maxTotal)
        .maxRetries(maxRetries)
        .apacheBufferSize(apacheBufferSize)
        .bufferSize(bufferSize)
        .outputFormat(outputFormat)
        .socketTimeout(socketTimeout)
        .connectionTimeout(connectionTimeout)
        .keepAliveTimeout(keepAliveTimeout)
        .additionalProperties(additionalProperties)
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
  private static Integer getPort(Properties properties, boolean ssl) {
    Integer port = getSetting(properties, FireboltSessionProperty.PORT);
    if (port == null) {
      port = ssl ? FIREBOLT_SSL_PROXY_PORT : FIREBOLT_NO_SSL_PROXY_PORT;
    }
    return port;
  }

  private static String getDatabase(Properties properties, boolean usePathAsDb, String path)
      throws IllegalArgumentException {
    String database = getSetting(properties, FireboltSessionProperty.DATABASE);
    if (usePathAsDb) {
      if ("/".equals(path) || StringUtils.isEmpty(path)) {
        return database;
      } else {
        Matcher m = DB_PATH_PATTERN.matcher(path);
        if (m.matches()) {
          return m.group(1);
        } else {
          throw new IllegalArgumentException(
              String.format("The database path provided is invalid %s", path));
        }
      }
    }
    return database;
  }

  private static List<Pair<String, String>> getFireboltCustomProperties(Properties properties) {
    return properties.entrySet().stream()
        .filter(entry -> !sessionPropertyKeys.contains(entry.getKey()))
        .map(entry -> new ImmutablePair<>((String) entry.getKey(), (String) entry.getValue()))
        .collect(Collectors.toList());
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

  public void addProperty(Pair<String, String> property) {
    additionalProperties.add(property);
  }
}
