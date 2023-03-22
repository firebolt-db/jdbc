package com.firebolt.jdbc.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mockStatic;

import java.sql.DriverPropertyInfo;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.internal.matchers.apachecommons.ReflectionEquals;

import com.firebolt.jdbc.connection.settings.FireboltSessionProperty;

class PropertyUtilTest {

	@Test
	void shouldGetPropertyInfo() {
		try (MockedStatic<FireboltSessionProperty> mocked = mockStatic(FireboltSessionProperty.class)) {
			List<FireboltSessionProperty> existingProperties = Arrays.asList(FireboltSessionProperty.ACCOUNT,
					FireboltSessionProperty.BUFFER_SIZE);
			mocked.when(FireboltSessionProperty::getNonDeprecatedProperties).thenReturn(existingProperties);
			DriverPropertyInfo accountDriverInfo = createExpectedDriverInfo(FireboltSessionProperty.ACCOUNT.getKey(),
					FireboltSessionProperty.ACCOUNT.getDescription(), "myAccount");
			DriverPropertyInfo bufferDriverInfo = createExpectedDriverInfo(FireboltSessionProperty.BUFFER_SIZE.getKey(),
					FireboltSessionProperty.BUFFER_SIZE.getDescription(), "1");
			DriverPropertyInfo[] expected = new DriverPropertyInfo[] { accountDriverInfo, bufferDriverInfo };

			for (int i = 0; i < expected.length; i++) {
				assertTrue(new ReflectionEquals(
						PropertyUtil.getPropertyInfo("jdbc:firebolt:Tutorial_11_04/?buffer_size=1&account=myAccount",
								new Properties())[i]).matches(expected[i]));
			}
		}
	}

	private DriverPropertyInfo createExpectedDriverInfo(String key, String description, String value) {
		DriverPropertyInfo driverPropertyInfo = new DriverPropertyInfo(key, value);
		driverPropertyInfo.required = false;
		driverPropertyInfo.description = description;
		driverPropertyInfo.choices = null;
		return driverPropertyInfo;
	}
}
