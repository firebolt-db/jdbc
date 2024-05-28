package com.firebolt.jdbc.type.lob;

import org.junit.jupiter.params.provider.Arguments;

import java.util.stream.Stream;

abstract class FireboltLobTest {
    protected static Stream<Arguments> replace() {
        return Stream.of(
                Arguments.of("hey, world!", "bye", 1, "bye, world!"),
                Arguments.of("hello, all!", "world!", 8, "hello, world!"),
                Arguments.of("hello, world!", "hello, world?", 1, "hello, world?"),
                Arguments.of("hello, world!", "!", 6, "hello! world!"),
                Arguments.of("hello, world!", "", 5, "hello, world!")
        );
    }

    protected static Stream<Arguments> partialReplace() {
        return Stream.of(
                Arguments.of("hey, world!", "bye", 1, 0, 3, "bye, world!"),
                Arguments.of("hello, all!", "world!", 8, 0, 6, "hello, world!"),
                Arguments.of("hello, world!", "hello, world?", 1, 0, 13, "hello, world?"),
                Arguments.of("hello, world!", "!", 6, 0, 1, "hello! world!"),
                Arguments.of("hello, world!", "", 5, 0, 0, "hello, world!"),

                Arguments.of("hello, world!", "world!", 8, 0, 5, "hello, world"),
                Arguments.of("hello, world!", "all people", 8, 4, 6, "hello, people"),
                Arguments.of("hello, all!", "bye people", 7, 3, 7, "hello, people")
        );
    }

    protected static Stream<Arguments> wrongReplace() {
        return Stream.of(
                Arguments.of("hello, all!", "people", -1, 0, 6),
                Arguments.of("hello, all!", "people", 1, -1, 6),
                Arguments.of("hello, all!", "people", 1, 0, 7),
                Arguments.of("hello, all!", "people", 1, 7, 0),
                Arguments.of("hello, all!", "people", 1, 5, 2)
        );
    }

    protected static Stream<Arguments> truncate() {
        return Stream.of(
                Arguments.of("hello, world!", 5, "hello"),
                Arguments.of("hello, world!", 13, "hello, world!"),
                Arguments.of("hello, world!", 0, ""),
                Arguments.of("hello, world!", 1, "h")
        );
    }

    protected static Stream<Arguments> position() {
        return Stream.of(
                Arguments.of("hello, world!", "hello", 1, 1),
                Arguments.of("hello, world!", "world", 1, 8),
                Arguments.of("hello, world!", "world", 3, 8),
                Arguments.of("hello, world!", "world", 8, 8),
                Arguments.of("hello, world!", "world", 9, -1),
                Arguments.of("hello, world!", "world", 0, -1),
                Arguments.of("hello, world!", "world", 14, -1),
                Arguments.of("hello, world!", "bye", 1, -1),
                Arguments.of("hello, world!", "hello", 5, -1),
                Arguments.of("1212", "112", 1, -1),
                Arguments.of("", "", 1, -1),
                Arguments.of("hello", "h", 0, -1),
                Arguments.of("hello", "", 1, 1)
        );
    }

    protected static Stream<Arguments> equalsAndHashCode() {
        return Stream.of(
                Arguments.of("one", "one", true),
                Arguments.of("one", "two", false),
                Arguments.of("two", "one", false),
                Arguments.of("", "", true),
                Arguments.of("something", "", false),
                Arguments.of("", "anything", false)
        );
    }
}
