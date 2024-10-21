package com.firebolt.jdbc.util;

import com.firebolt.jdbc.type.ParserVersion;
import lombok.CustomLog;

import java.sql.Connection;

@CustomLog
public class ConnectionParserResolver {
    private ConnectionParserResolver() {
        // Private constructor to prevent instantiation
    }

    public static ParserVersion getParserFromConnection(Connection connection) {
        if (connection instanceof com.firebolt.jdbc.connection.FireboltConnectionUserPassword) {
            return ParserVersion.LEGACY;
        } else if (connection instanceof com.firebolt.jdbc.connection.FireboltConnectionServiceSecret) {
            return ParserVersion.CURRENT;
        } else {
            log.warn("Connection instance is not recognized, assuming \"current\" sql string parser: "
                    + connection.getClass().getName());
            return ParserVersion.CURRENT;
        }
    }
}
