package integration.tests;

import integration.ConnectionInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ConnectionTest {
    /**
     * This test connects to specific engine with additional property {@code use_standard_sql} supported by user engine but
     * not supported by system engine used here to retrieve the data of user engine.
     * The test is needed because there were create connection to system engine by copying all given connection properties
     * while additional (custom) parameters should be ignored.
     * @throws SQLException if something went wrong
     */
    @Test
    void connectionWithAdditionalProperties() throws SQLException {
        ConnectionInfo params = integration.ConnectionInfo.getInstance();
        String url = format("jdbc:firebolt:%s?env=%s&engine=%s&account=%s&use_standard_sql=1", params.getDatabase(), params.getEnv(), params.getEngine(), params.getAccount());
        try(Connection connection = DriverManager.getConnection(url, params.getPrincipal(), params.getSecret())) {
            assertNotNull(connection);
        }
    }
}
