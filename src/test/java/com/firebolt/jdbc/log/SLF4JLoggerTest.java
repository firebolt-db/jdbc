package com.firebolt.jdbc.log;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class SLF4JLoggerTest {

    @Test
    void shouldLogWithCorrectLevel() {
        try (MockedStatic<LoggerFactory> mockedLoggerFactory = Mockito.mockStatic(LoggerFactory.class)) {
            Logger logger = mock(Logger.class);
            mockedLoggerFactory.when(() -> LoggerFactory.getLogger(anyString())).thenReturn(logger);
            SLF4JLogger slf4JLogger = new SLF4JLogger("logger");
            slf4JLogger.debug("This is a debug log");
            slf4JLogger.warn("This is a warning log");
            slf4JLogger.error("This is an error log");
            slf4JLogger.trace("This is a trace log");
            verify(logger).debug("This is a debug log");
            verify(logger).warn("This is a warning log");
            verify(logger).error("This is an error log");
            verify(logger).trace("This is a trace log");
        }
    }

    @Test
    void shouldLogWithArguments() {
        try (MockedStatic<LoggerFactory> mockedLoggerFactory = Mockito.mockStatic(LoggerFactory.class)) {
            Logger logger = mock(Logger.class);
            mockedLoggerFactory.when(() -> LoggerFactory.getLogger(anyString())).thenReturn(logger);
            SLF4JLogger slf4JLogger = new SLF4JLogger("logger");
            slf4JLogger.debug("This debug log has some arguments: {}, {}, {}", "arg1", "arg2", "arg3");
            slf4JLogger.warn("This warning log has some arguments: {}, {}, {}", "arg1", "arg2", "arg3");
            slf4JLogger.error("This error log has some arguments: {}, {}, {}", "arg1", "arg2", "arg3");
            slf4JLogger.trace("This trace log has some arguments: {}, {}, {}", "arg1", "arg2", "arg3");
            Object[] args = new Object[]{"arg1", "arg2", "arg3"};
            verify(logger).debug("This debug log has some arguments: {}, {}, {}", args);
            verify(logger).warn("This warning log has some arguments: {}, {}, {}", args);
            verify(logger).error("This error log has some arguments: {}, {}, {}", args);
            verify(logger).trace("This trace log has some arguments: {}, {}, {}", args);
        }
    }

    @Test
    void shouldLogWithThrowable() {
        try (MockedStatic<LoggerFactory> mockedLoggerFactory = Mockito.mockStatic(LoggerFactory.class)) {
            Logger logger = mock(Logger.class);
            mockedLoggerFactory.when(() -> LoggerFactory.getLogger(anyString())).thenReturn(logger);
            SLF4JLogger slf4JLogger = new SLF4JLogger("logger");
            IllegalArgumentException illegalArgumentException = new IllegalArgumentException();
            slf4JLogger.debug("This debug log has an exception", illegalArgumentException);
            slf4JLogger.warn("This warning log has an exception", illegalArgumentException);
            slf4JLogger.error("This error log has an exception", illegalArgumentException);
            slf4JLogger.trace("This trace log has an exception", illegalArgumentException);
            verify(logger).debug("This debug log has an exception", illegalArgumentException);
            verify(logger).warn("This warning log has an exception", illegalArgumentException);
            verify(logger).error("This error log has an exception", illegalArgumentException);
            verify(logger).trace("This trace log has an exception", illegalArgumentException);
        }
    }

}