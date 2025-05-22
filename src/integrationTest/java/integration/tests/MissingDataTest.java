package integration.tests;

import com.firebolt.jdbc.testutils.TestTag;
import integration.ConnectionInfo;
import integration.IntegrationTest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MissingDataTest extends IntegrationTest {
    @Test
    @Tag(TestTag.V2)
    void missingAccount() throws SQLException {
        ConnectionInfo current = integration.ConnectionInfo.getInstance();
        try (Connection good = DriverManager.getConnection(current.toJdbcUrl(), current.getPrincipal(), current.getSecret())) {
            assertNotNull(good);
        }

        ConnectionInfo noAccount = new ConnectionInfo(current.getPrincipal(), current.getSecret(),
                current.getEnv(), current.getDatabase(), null, current.getEngine(), current.getApi());
        assertThrows(SQLException.class, () -> DriverManager.getConnection(noAccount.toJdbcUrl(), noAccount.getPrincipal(), noAccount.getSecret()));
    }
}
