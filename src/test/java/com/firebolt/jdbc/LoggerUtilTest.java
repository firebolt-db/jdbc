package com.firebolt.jdbc;

import com.firebolt.jdbc.log.FireboltLogger;
import com.firebolt.jdbc.log.SLF4JLogger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class LoggerUtilTest {

    @Test
    void shouldGetSLF4JLoggerWhenAvailable() {
        FireboltLogger fireboltLogger = LoggerUtil.getLogger("myLogger");
        //Should be true since SLF4J is available
        assertTrue(fireboltLogger instanceof SLF4JLogger);
    }
}