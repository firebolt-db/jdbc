package com.firebolt.jdbc.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class VersionUtilTest {

	@Test
	void shouldGetDriverMajorVersion() {
		assertEquals(3, VersionUtil.getMajorDriverVersion());
	}

	@Test
	void shouldGetDriverMinorVersion() {
		int minorVersion = VersionUtil.getDriverMinorVersion();
		assertTrue(minorVersion >= 0 && minorVersion < 100);
	}

	@Test
	void shouldGetProjectVersion() {
		assertTrue(VersionUtil.getDriverVersion().matches("3\\.\\d+\\.\\d+"));
	}

	@Test
	void shouldGetMinorVersionFromString() {
		assertEquals(54, VersionUtil.extractMinorVersion("123.54.13"));
	}

	@Test
	void shouldGetMajorVersionFromString() {
		assertEquals(123, VersionUtil.extractMajorVersion("123.54.13"));
	}

	@Test
	void shouldGet0WhenMajorVersionCannotBeFound() {
		assertEquals(0, VersionUtil.extractMinorVersion("123a54b13"));
	}

	@Test
	void shouldGet0WhenMinorVersionCannotBeFound() {
		assertEquals(0, VersionUtil.extractMajorVersion("123a54b13"));
	}
}
