package com.firebolt.jdbc.log;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.logging.Level;
import java.util.logging.Logger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class JdkLoggerTest {

    @Test
    void shouldLogWithCorrectLevel() {
        try (MockedStatic<Logger> mockedLogger = Mockito.mockStatic(Logger.class)) {
            Logger logger = mock(Logger.class);
            mockedLogger.when(() -> java.util.logging.Logger.getLogger(any())).thenReturn(logger);
            JdkLogger jdkLogger = new JdkLogger("myTest");
            jdkLogger.debug("This is a debug log");
            jdkLogger.warn("This is a warning log");
            jdkLogger.error("This is an error log");
            jdkLogger.trace("This is a trace log");
            verify(logger).log(Level.FINE, "This is a debug log");
            verify(logger).log(Level.WARNING, "This is a warning log");
            verify(logger).log(Level.SEVERE, "This is an error log");
            verify(logger).log(Level.FINEST, "This is a trace log");
        }
    }

    @Test
    void shouldLogWithCorrectArgumentIndexes() {
        try (MockedStatic<Logger> mockedLogger = Mockito.mockStatic(Logger.class)) {
            Logger logger = mock(Logger.class);
            mockedLogger.when(() -> java.util.logging.Logger.getLogger(any())).thenReturn(logger);
            JdkLogger jdkLogger = new JdkLogger("myTest");
            jdkLogger.debug("This debug log has some arguments: {}, {}, {}", "arg1", "arg2", "arg3");
            jdkLogger.warn("This warning log has some arguments: {}, {}, {}", "arg1", "arg2", "arg3");
            jdkLogger.error("This error log has some arguments: {}, {}, {}", "arg1", "arg2", "arg3");
            jdkLogger.trace("This trace log has some arguments: {}, {}, {}", "arg1", "arg2", "arg3");
            Object[] args = new Object[]{"arg1", "arg2", "arg3"};
            verify(logger).log(Level.FINE, "This debug log has some arguments: {0}, {1}, {2}", args);
            verify(logger).log(Level.WARNING, "This warning log has some arguments: {0}, {1}, {2}", args);
            verify(logger).log(Level.SEVERE, "This error log has some arguments: {0}, {1}, {2}", args);
            verify(logger).log(Level.FINEST, "This trace log has some arguments: {0}, {1}, {2}", args);
        }
    }

    @Test
    void shouldLogWithThrowable() {
        try (MockedStatic<Logger> mockedLogger = Mockito.mockStatic(Logger.class)) {
            Logger logger = mock(Logger.class);
            mockedLogger.when(() -> java.util.logging.Logger.getLogger(any())).thenReturn(logger);
            JdkLogger jdkLogger = new JdkLogger("myTest");
            IllegalArgumentException illegalArgumentException = new IllegalArgumentException();
            jdkLogger.debug("This debug log has an exception", illegalArgumentException);
            jdkLogger.warn("This warning log has an exception", illegalArgumentException);
            jdkLogger.error("This error log has an exception", illegalArgumentException);
            jdkLogger.trace("This trace log has an exception", illegalArgumentException);
            verify(logger).log(Level.FINE, "This debug log has an exception", illegalArgumentException);
            verify(logger).log(Level.WARNING, "This warning log has an exception", illegalArgumentException);
            verify(logger).log(Level.SEVERE, "This error log has an exception", illegalArgumentException);
            verify(logger).log(Level.FINEST, "This trace log has an exception", illegalArgumentException);
        }
    }
}