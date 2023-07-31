package com.firebolt.jdbc.metadata;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ReadOnlyChecker {
    private final Connection connection;
    private final String parameter;
    private final String query;
    private Boolean readOnly;

    public ReadOnlyChecker(Connection connection, String parameter, String query) {
        this.connection = connection;
        this.parameter = parameter;
        this.query = query;
    }


    public boolean isReadOnly() throws SQLException {
        if (readOnly == null) {
            try (PreparedStatement ps = this.connection.prepareStatement(query)) {
                ps.setString(1, parameter);
                try (ResultSet rs = ps.executeQuery()) {
                    readOnly = rs.next() && rs.getBoolean(1);
                }
            }
        }
        return readOnly;
    }
}
