package com.firebolt.jdbc.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.firebolt.jdbc.log.FireboltLogger;
import com.firebolt.jdbc.log.SLF4JLogger;

import java.io.ByteArrayInputStream;
import java.io.IOException;

class LoggerUtilTest {

	@Test
	void shouldGetSLF4JLoggerWhenAvailable() {
		FireboltLogger fireboltLogger = LoggerUtil.getLogger("myLogger");
		// Should be true since SLF4J is available
		assertTrue(fireboltLogger instanceof SLF4JLogger);
	}

	@Test
	void logInputStream() throws IOException {
		String message = "hello";
		assertEquals(message, new String(LoggerUtil.logInputStream(new ByteArrayInputStream(message.getBytes())).readAllBytes()));
	}
}