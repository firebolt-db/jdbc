package com.firebolt.jdbc.metadata;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ReadOnlyCheckerTest {
    @ParameterizedTest
    @CsvSource({
            "false,,false",
            "true,false,false",
            "true,true,true"
    })
    void test(boolean hasNext, Boolean value, boolean expectedIsReadOnly) throws SQLException {
        String query = "select count(*)=0 from something where p = ?";
        Connection conn = mock(Connection.class);
        PreparedStatement ps = mock(PreparedStatement.class);
        when(conn.prepareStatement(query)).thenReturn(ps);
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(hasNext);
        if (hasNext) {
            when(rs.getBoolean(1)).thenReturn(value);
        }
        when(ps.executeQuery()).thenReturn(rs);
        assertEquals(expectedIsReadOnly, new ReadOnlyChecker(conn, "id", query).isReadOnly());
    }
}