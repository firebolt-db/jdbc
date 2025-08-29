package integration.tests.datatype;

import com.firebolt.jdbc.testutils.TestTag;
import integration.IntegrationTest;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag(TestTag.CORE)
@Tag(TestTag.V2)
@Disabled // enable it back when FIR-48811 is fixed
public class BigDecimalTest extends IntegrationTest {

    @BeforeEach
    void beforeEach() throws SQLException {
        // Create a test table for the large data insertion test
        try (Connection connection = createConnection()) {
            connection.createStatement().execute("DROP TABLE IF EXISTS bigdecimal_table");
            connection.createStatement().execute("CREATE TABLE IF NOT EXISTS bigdecimal_table (id INT, value NUMERIC)");
        }
    }

    @AfterEach
    void afterEach() throws SQLException {
        // drop the table
        try (Connection connection = createConnection()) {
            connection.createStatement().execute("DROP TABLE IF EXISTS bigdecimal_table");
        }
    }

    @Test
    void canInsertBigDecimalUsingPreparedStatement() throws SQLException {
        try (Connection connection = createConnection();
             PreparedStatement preparedStatement = connection.prepareStatement("INSERT into bigdecimal_table(id, value) values(?,?);");
             Statement verificationStatement = connection.createStatement()) {

            preparedStatement.setInt(1, 1);
            preparedStatement.setBigDecimal(2, new BigDecimal("12345.679"));
            preparedStatement.addBatch();

            // should be able to set it as a string
            preparedStatement.setInt(1, 2);
            preparedStatement.setString(2, "12345.679");
            preparedStatement.addBatch();

            // Record with maximum precision and scale (38,9) - 29 digits before decimal, 9 after
            preparedStatement.setInt(1, 3);
            preparedStatement.setBigDecimal(2, new BigDecimal("99999999999999999999999999999.123456789"));
            preparedStatement.addBatch();

            preparedStatement.setInt(1, 3);
            preparedStatement.setString(2, "99999999999999999999999999999.123456789");
            preparedStatement.addBatch();

            preparedStatement.executeBatch();

            List<BigDecimal> expectedValues = List.of(
                    new BigDecimal("12345.679000000"), // always use the whole scale of 9
                    new BigDecimal("12345.679000000"),
                    new BigDecimal("99999999999999999999999999999.123456789"),
                    new BigDecimal("99999999999999999999999999999.123456789")
            );

            ResultSet resultSet = verificationStatement.executeQuery("select * from bigdecimal_table order by id asc");
            int actualCount = 0;
            while (resultSet.next()) {
                assertEquals(expectedValues.get(actualCount), resultSet.getBigDecimal(2));
                actualCount++;
            }

            assertEquals(4, actualCount);
        }
    }
}
