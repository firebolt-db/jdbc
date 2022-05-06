package io.firebolt.jdbc.connection;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.net.URI;
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;

@Slf4j
@UtilityClass
public class FireboltJdbcUrlParser {

  private static final String JDBC_PREFIX = "jdbc:";

  public static Properties parse(String jdbcUrl) {
    URI uri = extractUriFromJdbcUrl(jdbcUrl);
    return parseUriQuery(uri);
  }

  private URI extractUriFromJdbcUrl(String jdbcConnectionString) {
    String cleanURI = jdbcConnectionString.substring(JDBC_PREFIX.length());
    return URI.create(cleanURI);
  }

  private static Properties parseUriQuery(URI uri) {
    Properties uriProperties = new Properties();
    String query = uri.getQuery();
    if (StringUtils.isNotEmpty(query)) {
      String[] queryKeyAndValues = StringUtils.split(query, "&");
      Arrays.stream(queryKeyAndValues)
          .map(s -> StringUtils.split(s, "="))
          .forEach(
              keyAndValue -> {
                if (keyAndValue.length == 2) {
                  uriProperties.put(keyAndValue[0], keyAndValue[1]);
                } else {
                  log.error(
                      "Cannot parse key-value pair from URI query. key-value: {}",
                      Arrays.toString(keyAndValue));
                }
              });
    }
    Optional.ofNullable(uri.getPath()).ifPresent(path -> uriProperties.put("path", path));
    Optional.ofNullable(uri.getHost()).ifPresent(host -> uriProperties.put("host", host));
    Optional.of(uri.getPort()).ifPresent(port -> uriProperties.put("port", port));
    return uriProperties;
  }
}
