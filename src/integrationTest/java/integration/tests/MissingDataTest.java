package integration.tests;

import integration.ConnectionInfo;
import integration.IntegrationTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class MissingDataTest extends IntegrationTest {
    @Test
    void missingAccount() throws SQLException {
        ConnectionInfo current = integration.ConnectionInfo.getInstance();
        try (Connection good = DriverManager.getConnection(current.toJdbcUrl(), current.getPrincipal(), current.getSecret())) {
            assertNotNull(good);
        }

        ConnectionInfo noAccount = new ConnectionInfo(current.getPrincipal(), current.getSecret(),
                current.getEnv(), current.getDatabase(), null, current.getEngine(), current.getApi(), current.getVersion());
        assertThrows(SQLException.class, () -> DriverManager.getConnection(noAccount.toJdbcUrl(), noAccount.getPrincipal(), noAccount.getSecret()));
    }
}
