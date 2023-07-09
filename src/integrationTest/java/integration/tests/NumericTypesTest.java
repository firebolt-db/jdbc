package integration.tests;

import integration.IntegrationTest;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class NumericTypesTest extends IntegrationTest {
    @Test
    void shouldHaveCorrectInfo() throws SQLException {
        try (Connection connection = this.createConnection("system");
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT 3::decimal")) {
            resultSet.next();
            assertEquals(9, resultSet.getMetaData().getScale(1));
            assertEquals(38, resultSet.getMetaData().getPrecision(1));
        }
    }
}
