package integration.tests;

import integration.IntegrationTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NullableValuesTest extends IntegrationTest {
    @BeforeEach
    void beforeAll() {
        executeStatementFromFile("/statements/nullable-types/ddl.sql");
    }

    @AfterEach
    void afterEach() {
        executeStatementFromFile("/statements/nullable-types/cleanup.sql");
    }

    @Test
    void preparedStatementUsingDefaultNulls() throws SQLException {
        try (Connection connection = createConnection();
             PreparedStatement insert = connection.prepareStatement("insert into nullable_types_test (id, year) values (?, ?)");
             PreparedStatement select = connection.prepareStatement("select * from nullable_types_test where id = ?")) {
            insert.setInt(1, 1);
            insert.setInt(2, 2023);
            assertFalse(insert.execute());

            select.setInt(1, 1);
            ResultSet rs = select.executeQuery();
            assertSelectResultSet(rs);
        }
    }

    @Test
    void preparedStatementUsingExplicitNulls() throws SQLException {
        try (Connection connection = createConnection();
             PreparedStatement insert = connection.prepareStatement("insert into nullable_types_test (id, year, ts, tstz, tsntz, content, success) values (?, ?, ?, ?, ?, ?, ?)");
             PreparedStatement select = connection.prepareStatement("select * from nullable_types_test where id = ?")) {
            insert.setInt(1, 1);
            insert.setInt(2, 2023);
            insert.setNull(3, Types.TIMESTAMP);
            insert.setNull(4, Types.TIMESTAMP);
            insert.setNull(5, Types.TIMESTAMP);
            insert.setNull(6, Types.VARCHAR);
            insert.setNull(7, Types.BOOLEAN);

            assertFalse(insert.execute());

            select.setInt(1, 1);
            ResultSet rs = select.executeQuery();
            assertSelectResultSet(rs);
        }
    }

    @Test
    void preparedStatementUsingExplicitTypedNulls() throws SQLException {
        try (Connection connection = createConnection();
             PreparedStatement insert = connection.prepareStatement("insert into nullable_types_test (id, year, ts, tstz, tsntz, content) values (?, ?, ?, ?, ?, ?)");
             PreparedStatement select = connection.prepareStatement("select * from nullable_types_test where id = ?")) {
            insert.setInt(1, 1);
            insert.setInt(2, 2023);

            insert.setTimestamp(3, null);
            insert.setTimestamp(4, null);
            insert.setTimestamp(5, null);
            insert.setString(6, null);
            // setBoolean does not accept null, so it is omitted here

            assertFalse(insert.execute());

            select.setInt(1, 1);
            ResultSet rs = select.executeQuery();
            assertSelectResultSet(rs);
        }
    }

    @Test
    void preparedStatementUsingStringValueContainsStringNull() throws SQLException {
        try (Connection connection = createConnection();
             PreparedStatement insert = connection.prepareStatement("insert into nullable_types_test (id, year, content) values (?, ?, ?)");
             PreparedStatement select = connection.prepareStatement("select * from nullable_types_test where id = ?")) {
            insert.setInt(1, 1);
            insert.setInt(2, 2023);
            insert.setString(3, "null"); // string "null" - not null value!
            assertFalse(insert.execute());

            select.setInt(1, 1);
            ResultSet rs = select.executeQuery();
            assertTrue(rs.next());
            assertEquals("null", rs.getString("content"));
            assertFalse(rs.next());
        }
    }


    private void assertSelectResultSet(ResultSet rs) throws SQLException {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertEquals(2023, rs.getInt("year"));
        assertNull(rs.getTimestamp("ts"));
        assertNull(rs.getTimestamp("tstz"));
        assertNull(rs.getTimestamp("tsntz"));
        assertNull(rs.getString("content"));
        assertNull(rs.getObject("success"));
        assertFalse(rs.getBoolean("success")); // null value is treated as false
        assertFalse(rs.next());
    }
}
