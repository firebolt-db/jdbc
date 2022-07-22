package com.firebolt.jdbc.connection;

import com.firebolt.jdbc.connection.settings.FireboltSessionProperty;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.net.URI;
import java.util.Optional;
import java.util.Properties;

@Slf4j
@UtilityClass
public class FireboltJdbcUrlUtil {

  public static final String JDBC_PREFIX = "jdbc:";

  public static Properties extractProperties(String jdbcUrl) {
    URI uri = extractUriFromJdbcUrl(jdbcUrl);
    return parseUriQueryPart(uri);
  }

  private URI extractUriFromJdbcUrl(String jdbcConnectionString) {
    String cleanURI = StringUtils.replace(jdbcConnectionString, JDBC_PREFIX, "");
    return URI.create(cleanURI);
  }

  private static Properties parseUriQueryPart(URI uri) {
    Properties uriProperties = new Properties();
    String query = uri.getQuery();
    if (StringUtils.isNotBlank(query)) {
      String[] queryKeyValues = query.split("&");
      for (String keyValue : queryKeyValues) {
        String[] keyValueTokens = keyValue.split("=");
        if (keyValueTokens.length == 2) {
          uriProperties.put(keyValueTokens[0], keyValueTokens[1]);
        } else {
          log.warn("Cannot parse key-pair: {}", keyValue);
        }
      }
    }
    Optional.ofNullable(uri.getPath()).ifPresent(path -> uriProperties.put(FireboltSessionProperty.PATH.getKey(), path));
    Optional.ofNullable(uri.getHost()).ifPresent(host -> uriProperties.put(FireboltSessionProperty.HOST.getKey(), host));
    Optional.of(uri.getPort()).filter(p -> !p.equals(-1)).ifPresent(port -> uriProperties.put(FireboltSessionProperty.PORT.getKey(), String.valueOf(port)));
    return uriProperties;
  }
}
