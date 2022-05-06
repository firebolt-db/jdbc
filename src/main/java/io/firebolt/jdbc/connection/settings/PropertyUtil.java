package io.firebolt.jdbc.connection.settings;

import io.firebolt.jdbc.connection.FireboltJdbcUrlParser;
import lombok.experimental.UtilityClass;

import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;

@UtilityClass
public class PropertyUtil {

  public static FireboltProperties extractFireboltProperties(
      String uri,
      Properties connectionSettings,
      Function<Properties, FireboltProperties> propertiesTransformer) {
    Properties uriProperties = FireboltJdbcUrlParser.parse(uri);
    Properties mergedProperties = mergeProperties(connectionSettings, uriProperties);
    return Optional.of(mergedProperties).map(propertiesTransformer).orElse(null);
  }

  private static Properties mergeProperties(
      Properties connectionSettings, Properties uriProperties) {
    Properties mergedProperties = new Properties();
    mergedProperties.putAll(uriProperties);
    mergedProperties.putAll(connectionSettings);
    return mergedProperties;
  }
}
