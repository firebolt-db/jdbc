package io.firebolt.jdbc;

import io.firebolt.jdbc.connection.FireboltJdbcUrlUtil;
import io.firebolt.jdbc.connection.settings.FireboltProperties;
import io.firebolt.jdbc.connection.settings.FireboltSessionProperty;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.sql.DriverPropertyInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

@Slf4j
@UtilityClass
public class PropertyUtil {

  private static final String LOCALHOST = "localhost";

  public static DriverPropertyInfo[] getPropertyInfo(String url, Properties properties) {
    try {
      Properties propertiesFromUrl = FireboltJdbcUrlUtil.extractProperties(url);
      for (Object key : propertiesFromUrl.keySet()) {
        properties.put(key, propertiesFromUrl.get(key.toString()));
      }
    } catch (Exception ex) {
      log.error("Could not extract properties from url {}", url, ex);
    }

    List<DriverPropertyInfo> result =
        new ArrayList<>(
            mapProperties(FireboltSessionProperty.getNonDeprecatedProperties(), properties));
    return result.toArray(new DriverPropertyInfo[0]);
  }

  public boolean isLocalDb(FireboltProperties fireboltProperties) {
    return StringUtils.equalsIgnoreCase(fireboltProperties.getHost(), LOCALHOST);
  }

  private static List<DriverPropertyInfo> mapProperties(
      List<FireboltSessionProperty> fireboltSessionProperties, Properties properties) {
    return fireboltSessionProperties.stream()
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
