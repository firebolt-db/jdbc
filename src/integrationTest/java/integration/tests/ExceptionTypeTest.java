package integration.tests;

import com.firebolt.jdbc.exception.ExceptionType;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.testutils.TestTag;
import integration.ConnectionInfo;
import integration.IntegrationTest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import lombok.CustomLog;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag(TestTag.V2)
@CustomLog
public class ExceptionTypeTest extends IntegrationTest {

    @BeforeAll
    void beforeAll() throws SQLException {
        // Create a test table for the large data insertion test
        try (Connection connection = createConnection()) {
            connection.createStatement().execute("DROP TABLE IF EXISTS large_data_test_table");
            connection.createStatement().execute("CREATE TABLE IF NOT EXISTS large_data_test_table (id INT, value TEXT)");
        }
    }

    @AfterAll
    void afterAll() throws SQLException {
        // drop the table
        try (Connection connection = createConnection()) {
            connection.createStatement().execute("DROP TABLE IF EXISTS large_data_test_table");
        }
    }

    @Test
    @Tag(TestTag.V2)
    void shouldThrowRequestBodyTooLargeExceptionForLargeData() throws SQLException {
        // Create a very large string that should trigger a 413 Request Body Too Large error
        // "Ā" is a unicode represented on 2 bytes. So it will be a 4MB value. But some accounts (automation) has an upper limit of 40MB
        // so create multiple statements.
        String largeValue = "Ā".repeat(2100000);

        // force the merge prepared statement so all will be executed in one go
        Map<String, String> connectionParams = Map.of(
                "merge_prepared_statement_batches", "true"
        );
        try (Connection connection = createConnection(ConnectionInfo.getInstance().getEngine(), connectionParams)) {
            String preparedStatementSql = "INSERT INTO large_data_test_table (id, value) VALUES (?, ?)";
            PreparedStatement preparedStatement = connection.prepareStatement(preparedStatementSql);

            // Add multiple large batches to ensure we exceed the request body size limit
            for (int i = 0; i < 10; i++) {
                preparedStatement.setInt(1, i);
                preparedStatement.setString(2, largeValue);
                preparedStatement.addBatch();
            }

            // Execute the batch and expect a REQUEST_BODY_TOO_LARGE exception (HTTP 413)
            FireboltException exception = assertThrows(FireboltException.class, () -> {
                preparedStatement.executeBatch();
            });

            // Assert that the exception type is REQUEST_BODY_TOO_LARGE (HTTP 413)
            assertEquals(ExceptionType.REQUEST_BODY_TOO_LARGE, exception.getType());
        }
    }

    @Test
    @Tag(TestTag.V2)
    void shouldNotThrowRequestBodyTooLargeExceptionForLargeDataWhenCompressionIsUsed() throws SQLException {
        // Create a very large string that should trigger a 413 Request Body Too Large error
        // "Ā" is a unicode represented on 2 bytes. So it will be a 4MB value. But some accounts (automation) has an upper limit of 40MB
        // so create multiple statements.
        String largeValue = "Ā".repeat(2100000);

        // force the merge prepared statement so all will be executed in one go, but not compress
        Map<String, String> connectionParams = Map.of(
                "merge_prepared_statement_batches", "true",
                "compress_request_payload", "true"
        );
        try (Connection connection = createConnection(ConnectionInfo.getInstance().getEngine(), connectionParams)) {
            String preparedStatementSql = "INSERT INTO large_data_test_table (id, value) VALUES (?, ?)";
            PreparedStatement preparedStatement = connection.prepareStatement(preparedStatementSql);

            // Add multiple large batches to ensure we exceed the request body size limit
            for (int i = 0; i < 10; i++) {
                preparedStatement.setInt(1, i);
                preparedStatement.setString(2, largeValue);
                preparedStatement.addBatch();
            }

            // can execute request
            int[] result = preparedStatement.executeBatch();
            assertEquals(10, result.length);
        }
    }
}
