package integration.tests;

import com.firebolt.jdbc.testutils.TestTag;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DiscoveryConnectionTest {

    private static final String DEFAULT_DISCOVERY_URL =
            "jdbc:firebolt://localhost:3473?ssl_mode=disable&database=integration_test_db";

    @Test
    @Tag(TestTag.CORE)
    void shouldConnectThroughDiscoveryBasedLocalFirebolt() throws SQLException {
        String jdbcUrl = System.getProperty("firebolt.discovery.jdbc_url", DEFAULT_DISCOVERY_URL);
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
             ResultSet resultSet = connection.createStatement().executeQuery("SELECT 1")) {
            assertTrue(resultSet.next());
            assertEquals(1, resultSet.getInt(1));
            assertFalse(resultSet.next());
        }
    }
}
