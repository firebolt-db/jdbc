package io.firebolt.jdbc.connection.settings;

import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class PropertiesToFireboltPropertiesTransformer
    implements Function<Properties, FireboltProperties> {

  private static final Pattern DB_PATH_PATTERN = Pattern.compile("/([a-zA-Z0-9_*\\-]+)");
  private static final int FIREBOLT_SSL_PROXY_PORT = 443;
  private static final int FIREBOLT_NO_SSL_PROXY_PORT = 9090;
  private static final Set<String> connectionSettingKeys =
      Arrays.stream(FireboltSessionProperty.values())
          .map(FireboltSessionProperty::getKey)
          .collect(Collectors.toSet());
  private static final Set<String> queryParams =
      Arrays.stream(FireboltSessionProperty.values())
          .map(FireboltSessionProperty::getKey)
          .collect(Collectors.toSet());

  @Override
  public FireboltProperties apply(Properties properties) {
    boolean ssl = getSetting(properties, FireboltSessionProperty.SSL);
    String sslRootCertificate =
        getSetting(properties, FireboltSessionProperty.SSL_ROOT_CERTIFICATE);
    String sslMode = getSetting(properties, FireboltSessionProperty.SSL_MODE);
    boolean usePathAsDb = getSetting(properties, FireboltSessionProperty.USE_PATH_AS_DB);
    Integer compress = getSetting(properties, FireboltSessionProperty.COMPRESS);
    boolean decompress = getSetting(properties, FireboltSessionProperty.DECOMPRESS);
    Integer useConnectionPool = getSetting(properties, FireboltSessionProperty.USE_CONNECTION_POOL);
    String user = getSetting(properties, FireboltSessionProperty.USER);
    String password = getSetting(properties, FireboltSessionProperty.PASSWORD);
    String path = getSetting(properties, FireboltSessionProperty.PATH);
    String engine = getSetting(properties, FireboltSessionProperty.ENGINE);
    String account = getSetting(properties, FireboltSessionProperty.ACCOUNT);
    int defaultMaxPerRoute = getSetting(properties, FireboltSessionProperty.DEFAULT_MAX_PER_ROUTE);
    int timeToLiveMillis = getSetting(properties, FireboltSessionProperty.TIME_TO_LIVE_MILLIS);
    int validateAfterInactivityMillis =
        getSetting(properties, FireboltSessionProperty.VALIDATE_AFTER_INACTIVITY_MILLIS);
    int maxTotal = getSetting(properties, FireboltSessionProperty.MAX_TOTAL);
    int maxRetries = getSetting(properties, FireboltSessionProperty.MAX_RETRIES);
    int bufferSize = getSetting(properties, FireboltSessionProperty.BUFFER_SIZE);
    int apacheBufferSize = getSetting(properties, FireboltSessionProperty.APACHE_BUFFER_SIZE);
    String outputFormat = getSetting(properties, FireboltSessionProperty.OUTPUT_FORMAT);
    int socketTimeout = getSetting(properties, FireboltSessionProperty.SOCKET_TIMEOUT);
    int connectionTimeout = getSetting(properties, FireboltSessionProperty.CONNECTION_TIMEOUT);
    int keepAliveTimeout = getSetting(properties, FireboltSessionProperty.KEEP_ALIVE_TIMEOUT);
    String host = getHost(properties, ssl);
    Integer port = getPort(properties, ssl);
    String database = getDatabase(properties, usePathAsDb, path);
    Properties fireboltProperties = getFireboltCustomProperties(properties);

    properties.entrySet().stream()
        .filter(entry -> !connectionSettingKeys.contains(entry.getKey()))
        .filter(entry -> !queryParams.contains(entry.getKey()))
        .forEach(entry -> fireboltProperties.put(entry.getKey(), entry.getValue()));

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
        .customProperties(fireboltProperties)
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
        .build();
  }

  private String getHost(Properties properties, boolean ssl) {
    String host = getSetting(properties, FireboltSessionProperty.HOST);
    if (StringUtils.isEmpty(host)) {
      throw new IllegalArgumentException("Invalid host: The host is missing or empty");
    } else {
      host = ssl ? String.format("https://%s", host) : String.format("http://%s", host);
    }
    return host;
  }

  @NotNull
  private Integer getPort(Properties properties, boolean ssl) {
    Integer port = getSetting(properties, FireboltSessionProperty.PORT);
    if (port == null) {
      port = ssl ? FIREBOLT_SSL_PROXY_PORT : FIREBOLT_NO_SSL_PROXY_PORT;
    }
    return port;
  }

  private String getDatabase(Properties properties, boolean usePathAsDb, String path)
      throws IllegalArgumentException {
    String database = getSetting(properties, FireboltSessionProperty.DATABASE);
    if (usePathAsDb) {
      if ("/".equals(path)) {
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

  private Properties getFireboltCustomProperties(Properties properties) {
    Properties fireboltProperties = new Properties();
    properties.entrySet().stream()
        .filter(entry -> !connectionSettingKeys.contains(entry.getKey()))
        .filter(entry -> !queryParams.contains(entry.getKey()))
        .forEach(entry -> fireboltProperties.put(entry.getKey(), entry.getValue()));

    return fireboltProperties;
  }

  private <T> T getSetting(Properties info, FireboltSessionProperty param) {
    return getSetting(info, param.getKey(), param.getDefaultValue(), param.getClazz());
  }

  @SuppressWarnings("unchecked")
  private <T> T getSetting(Properties info, String key, Object defaultValue, Class<?> clazz) {
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
}
