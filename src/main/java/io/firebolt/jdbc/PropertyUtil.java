package io.firebolt.jdbc;

import io.firebolt.jdbc.connection.FireboltJdbcUrlUtil;
import io.firebolt.jdbc.connection.settings.FireboltSessionProperty;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.sql.DriverPropertyInfo;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@UtilityClass
public class PropertyUtil {

  public static DriverPropertyInfo[] getPropertyInfo(String url, Properties properties) {
    try {
      Properties propertiesFromUrl = FireboltJdbcUrlUtil.extractProperties(url);
      for (Object key : propertiesFromUrl.keySet()) {
        properties.put(key, propertiesFromUrl.get(key.toString()));
      }
    } catch (Exception ex) {
      log.error("Could not extract properties from url {}", url, ex);
    }
    List<DriverPropertyInfo> result = new ArrayList<>(mapProperties(FireboltSessionProperty.values(), properties));
    return result.toArray(new DriverPropertyInfo[0]);
  }

  private static List<DriverPropertyInfo> mapProperties(
      FireboltSessionProperty[] fireboltSessionProperties, Properties properties) {
    return Arrays.stream(fireboltSessionProperties)
        .map(
            fireboltProperty -> {
              DriverPropertyInfo driverPropertyInfo =
                  new DriverPropertyInfo(
                      fireboltProperty.getKey(),
                      getValueForFireboltSessionProperty(properties, fireboltProperty));
              driverPropertyInfo.required = false;
              driverPropertyInfo.description = fireboltProperty.getDescription();
              driverPropertyInfo.choices = fireboltProperty.getPossibleValues();
              return driverPropertyInfo;
            })
        .collect(Collectors.toList());
  }

  private static String getValueForFireboltSessionProperty(
      Properties properties, FireboltSessionProperty fireboltSessionProperty) {
    Optional<String> value =
        Optional.ofNullable(properties.getProperty(fireboltSessionProperty.getKey()));

    return value.orElseGet(
        () ->
            Optional.ofNullable(fireboltSessionProperty.getDefaultValue())
                .map(Object::toString)
                .orElse(null));
  }
}
