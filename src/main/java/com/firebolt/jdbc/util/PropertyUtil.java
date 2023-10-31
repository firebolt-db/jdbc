package com.firebolt.jdbc.util;

import com.firebolt.jdbc.connection.UrlUtil;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.connection.settings.FireboltSessionProperty;
import lombok.CustomLog;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.StringUtils;

import java.sql.DriverPropertyInfo;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@CustomLog
@UtilityClass
public class PropertyUtil {

	private static final String LOCALHOST = "localhost";

	/**
	 * Returns an array containing the properties used by the driver
	 * 
	 * @param url        the JDBC url
	 * @param properties the properties
	 * @return an array containing the properties used by the driver
	 */
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties properties) {
		try {
			Properties propertiesFromUrl = UrlUtil.extractProperties(url);
			for (Object key : propertiesFromUrl.keySet()) {
				properties.put(key, propertiesFromUrl.get(key.toString()));
			}
		} catch (Exception ex) {
			log.error("Could not extract properties from url {}", url, ex);
		}

		List<DriverPropertyInfo> result = new ArrayList<>(
				mapProperties(FireboltSessionProperty.getNonDeprecatedProperties(), properties));
		return result.toArray(new DriverPropertyInfo[0]);
	}

	/**
	 * Returns true if the host property is localhost
	 * 
	 * @param fireboltProperties the properties
	 * @return true if the host property is localhost
	 */
	public boolean isLocalDb(FireboltProperties fireboltProperties) {
		return StringUtils.equalsIgnoreCase(fireboltProperties.getHost(), LOCALHOST);
	}

	private List<DriverPropertyInfo> mapProperties(List<FireboltSessionProperty> fireboltSessionProperties,
			Properties properties) {
		return fireboltSessionProperties.stream().map(fireboltProperty -> {
			Entry<String, String> property = getValueForFireboltSessionProperty(properties, fireboltProperty);
			DriverPropertyInfo driverPropertyInfo = new DriverPropertyInfo(property.getKey(), property.getValue());
			driverPropertyInfo.required = false;
			driverPropertyInfo.description = fireboltProperty.getDescription();
			driverPropertyInfo.choices = fireboltProperty.getPossibleValues();
			return driverPropertyInfo;
		}).collect(Collectors.toList());
	}

	private Entry<String, String> getValueForFireboltSessionProperty(Properties properties, FireboltSessionProperty fireboltSessionProperty) {
		String strDefaultValue = Optional.ofNullable(fireboltSessionProperty.getDefaultValue()).map(Object::toString).orElse(null);
		return Stream.concat(Stream.of(fireboltSessionProperty.getKey()), Arrays.stream(fireboltSessionProperty.getAliases()))
				.filter(key -> properties.getProperty(key) != null)
				.map(key -> new SimpleEntry<>(key, properties.getProperty(key)))
				.findFirst()
				.orElseGet(() -> new SimpleEntry<>(fireboltSessionProperty.getKey(), strDefaultValue));
	}
}
