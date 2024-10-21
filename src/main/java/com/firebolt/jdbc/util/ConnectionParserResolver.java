package com.firebolt.jdbc.util;

import com.firebolt.jdbc.type.ParserVersion;
import lombok.CustomLog;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

@CustomLog
public class ConnectionParserResolver {
    // Map of classes to parser versions
    private static final Map<Class<?>, ParserVersion> PARSER_MAP = new HashMap<>(
            Map.of(
                    com.firebolt.jdbc.connection.FireboltConnectionUserPassword.class, ParserVersion.LEGACY,
                    com.firebolt.jdbc.connection.FireboltConnectionServiceSecret.class, ParserVersion.CURRENT));

    private ConnectionParserResolver() {
        // Private constructor to prevent instantiation
    }

    public static ParserVersion getParserFromConnection(Connection connection) {
        return PARSER_MAP.getOrDefault(connection.getClass(), ParserVersion.CURRENT);
    }
}
