package com.firebolt.jdbc.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Properties;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class UrlUtilTest {


	@ParameterizedTest
	@CsvSource({
			"jdbc:firebolt:Tutorial_11_05/?host=api.dev.firebolt.io&account=firebolt, Tutorial_11_05, api.dev.firebolt.io, firebolt",
			"jdbc:firebolt:Tutorial_11_05?host=api.dev.firebolt.io&account=firebolt, Tutorial_11_05, api.dev.firebolt.io, firebolt",
			"jdbc:firebolt:/?&host=api.dev.firebolt.io&account=firebolt, '', api.dev.firebolt.io, firebolt"
	})
	void shouldGetAllPropertiesFromUri(String uri, String expectedPath, String expectedHost, String expectedAccount) {
		Properties properties = UrlUtil.extractProperties(uri);
		Properties expectedProperties = new Properties();
		expectedProperties.put("path", expectedPath);
		expectedProperties.put("host", expectedHost);
		expectedProperties.put("account", expectedAccount);
		assertEquals(expectedProperties, properties);
	}

}
