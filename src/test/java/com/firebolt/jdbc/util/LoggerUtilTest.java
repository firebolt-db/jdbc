package com.firebolt.jdbc.util;

import org.junit.jupiter.api.Test;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LoggerUtilTest {

	@Test
	void shouldGetSLF4JLoggerWhenAvailable() {
		Logger fireboltLogger = LoggerUtil.getRootLogger();
		// Should be true since SLF4J is available
		assertTrue(Arrays.stream(fireboltLogger.getHandlers()).anyMatch(handler -> handler instanceof SLF4JBridgeHandler));
	}

	@Test
	void logInputStream() throws IOException {
		String message = "hello";
		assertEquals(message, new String(LoggerUtil.logInputStream(new ByteArrayInputStream(message.getBytes())).readAllBytes()));
	}
}