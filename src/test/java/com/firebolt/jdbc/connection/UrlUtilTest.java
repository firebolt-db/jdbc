package com.firebolt.jdbc.connection;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

	@Test
	void createUrl() throws MalformedURLException {
		String spec = "http://myhost/path?x=1&y=2";
		assertEquals(new URL(spec), UrlUtil.createUrl(spec));
	}

	@Test
	void createBadUrl() {
		assertEquals(MalformedURLException.class, assertThrows(IllegalArgumentException.class, () -> UrlUtil.createUrl("not url")).getCause().getClass());
	}

	@ParameterizedTest
	@ValueSource(strings = {"http://host", "http://host/", "http://host/?", "http://host?", "http://host:8080", "http://host:8080/", "http://host:8080/?", "http://host:8080?"})
	void getQueryParametersNoParameters(String spec) throws MalformedURLException {
		assertEquals(Map.of(), UrlUtil.getQueryParameters(new URL(spec)));
	}

	@ParameterizedTest
	@ValueSource(strings = {
			"http://the-host.com?database&engine=diesel&format=json", // set each parameter only once
			"http://the-host.com?database&format=xml&engine=benzine&engine=diesel&format=json" // override parameters
	})
	void getQueryParameters() throws MalformedURLException {
		Map<String, String> expected = new HashMap<>();
		expected.put("engine", "diesel");
		expected.put("format", "json");
		assertEquals(expected, UrlUtil.getQueryParameters(new URL("http://the-host.com?database&engine=diesel&format=json")));
	}
}
