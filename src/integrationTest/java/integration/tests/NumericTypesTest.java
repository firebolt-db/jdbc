package integration.tests;

import integration.CommonIntegrationTest;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class NumericTypesTest extends CommonIntegrationTest {
    @Test
    void shouldHaveCorrectInfo() throws SQLException {
        try (Connection connection = createConnection(null);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT 3::decimal")) {
            resultSet.next();
            assertEquals(9, resultSet.getMetaData().getScale(1));
            assertEquals(38, resultSet.getMetaData().getPrecision(1));
        }
    }

    @Test
    void shouldHandleLargeDecimals() throws SQLException {
        String sql = "SELECT 22345678901234567890123456789.123456789::decimal(38, 9);";
        try (Connection connection = createConnection(null);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            resultSet.next();
            assertEquals(new BigDecimal("12345678901234567890123456789.123456789"), resultSet.getBigDecimal(1));
        }
    }
}
