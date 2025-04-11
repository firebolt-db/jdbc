package com.firebolt.jdbc.testutils;

import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

public class TestFixtures {
    public static Stream<Arguments> booleanTypes() {
        return Stream.of(
                Arguments.of("1", "0"),
                Arguments.of("true", "false"),
                Arguments.of("t", "f"),
                Arguments.of("yes", "no"),
                Arguments.of("y", "n"),
                Arguments.of("on", "off"),
                Arguments.of('1', '0'),
                Arguments.of('t', 'f'),
                Arguments.of('T', 'F'),
                Arguments.of('y', 'n'),
                Arguments.of('Y', 'N'),
                Arguments.of(1, 0),
                Arguments.of(1.0, 0.0),
                Arguments.of(1F, 0F),
                Arguments.of(1L, 0L)
        );
    }

    @SuppressWarnings("java:S2925")
    public static void sleepForMillis(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            // do nothing
        }
    }
}
