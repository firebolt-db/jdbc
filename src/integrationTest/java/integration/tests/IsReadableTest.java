package integration.tests;

import integration.IntegrationTest;
import lombok.CustomLog;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@CustomLog
public class IsReadableTest extends IntegrationTest {
    @Test
    void connection() throws SQLException {
        try (Connection connection = createConnection()) {
            assertFalse(connection.isReadOnly());
        }
    }

    @Test
    void databaseMetadata() throws SQLException {
        try (Connection connection = createConnection()) {
            assertFalse(connection.getMetaData().isReadOnly());
        }
    }

    @Test
    void systemConnection() throws SQLException {
        try (Connection connection = createConnection(getSystemEngineName())) {
            assertTrue(connection.isReadOnly());
        }
    }

    @Test
    void systemDatabaseMetadata() throws SQLException {
        try (Connection connection = createConnection(getSystemEngineName())) {
            assertTrue(connection.getMetaData().isReadOnly());
        }
    }
}
