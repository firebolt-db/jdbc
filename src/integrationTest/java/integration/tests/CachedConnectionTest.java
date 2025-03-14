package integration.tests;

import integration.IntegrationTest;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class CachedConnectionTest extends IntegrationTest {

    private static final String CACHED_TEST_ENGINE_NAME = "cached_test_second_engine";
    private static final String CACHED_TEST_DATABASE_NAME = "cached_test_second_db";

    @BeforeAll
    void beforeAll() {
        executeStatementFromFile("/statements/cache-connection/ddl.sql");
    }

    @AfterAll
    void afterEach() {
        executeStatementFromFile("/statements/cache-connection/cleanup.sql");
    }

    @Test
    @Tag("v2")
    @Tag("slow")
    void createTwoConnections() throws SQLException {
        // create a connection on the first engine and database
        try (Connection connection = createConnection()) {
            ResultSet rs = connection.createStatement().executeQuery("SELECT 101");
            assertTrue(rs.next());
        }

        // create a connection on the second engine and database
        try (Connection connection = createConnection(CACHED_TEST_ENGINE_NAME, CACHED_TEST_DATABASE_NAME)) {
            ResultSet rs = connection.createStatement().executeQuery("SELECT 102");
            assertTrue(rs.next());
        }

        // create a connection back on the first engine and database
        try (Connection connection = createConnection()) {
            ResultSet rs = connection.createStatement().executeQuery("SELECT 103");
            assertTrue(rs.next());

            sle
        }

        // create a connection on the second engine and database
        try (Connection connection = createConnection(CACHED_TEST_ENGINE_NAME, CACHED_TEST_DATABASE_NAME)) {
            ResultSet rs = connection.createStatement().executeQuery("SELECT 104");
            assertTrue(rs.next());
        }


    }

}
