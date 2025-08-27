package integration.tests.datatype;

import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.testutils.TestTag;
import integration.IntegrationTest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(TestTag.CORE)
@Tag(TestTag.V2)
public class BooleanTest extends IntegrationTest {

    @BeforeEach
    void beforeEach() throws SQLException {
        // Create a test table for the large data insertion test
        try (Connection connection = createConnection()) {
            connection.createStatement().execute("DROP TABLE IF EXISTS boolean_table");
            connection.createStatement().execute("CREATE TABLE IF NOT EXISTS boolean_table (id INT, value BOOLEAN)");
        }
    }

    @AfterEach
    void afterEach() throws SQLException {
        // drop the table
        try (Connection connection = createConnection()) {
            connection.createStatement().execute("DROP TABLE IF EXISTS boolean_table");
        }
    }

    @Test
    void canUsePreparedStatementsToInsertTrueValueInBooleanColumn() throws SQLException {
        try (Connection connection = createConnection();
             PreparedStatement preparedStatement = connection.prepareStatement("INSERT into boolean_table(id, value) values(?,?);");
             Statement verificationStatement = connection.createStatement()) {
            // string value 1 is considered true
            preparedStatement.setInt(1, 1);
            preparedStatement.setString(2, "1");
            preparedStatement.addBatch();

            // any positive integer as string is also considered true
            preparedStatement.setInt(1, 2);
            preparedStatement.setString(2, "456");
            preparedStatement.addBatch();

            // even using set object with any positive integer as string is considered true
            preparedStatement.setInt(1, 3);
            preparedStatement.setObject(2, "789");
            preparedStatement.addBatch();

            // true, t or any case of true is considered true (TRUE, truE, True, etc)
            preparedStatement.setInt(1, 4);
            preparedStatement.setString(2, "true");
            preparedStatement.addBatch();

            preparedStatement.setInt(1, 5);
            preparedStatement.setString(2, "TRUE");
            preparedStatement.addBatch();

            // even with set object should work
            preparedStatement.setInt(1, 6);
            preparedStatement.setObject(2, "True");
            preparedStatement.addBatch();

            // smaller case t
            preparedStatement.setInt(1, 7);
            preparedStatement.setString(2, "t");
            preparedStatement.addBatch();

            // upper case T
            preparedStatement.setInt(1, 8);
            preparedStatement.setString(2, "T");
            preparedStatement.addBatch();

            // even a double value as a string would still be considered as true
            preparedStatement.setInt(1, 9);
            preparedStatement.setString(2, "0.1123");
            preparedStatement.addBatch();

            preparedStatement.setInt(1, 10);
            preparedStatement.setBoolean(2, true);
            preparedStatement.addBatch();

            preparedStatement.executeBatch();

            ResultSet resultSet = verificationStatement.executeQuery("select * from boolean_table");
            int actualCount = 0;
            while (resultSet.next()) {
                actualCount++;
                assertTrue(resultSet.getBoolean(2));
            }

            assertEquals(10, actualCount);
        }
    }

    @Test
    void canUsePreparedStatementsToInsertFalseValueInBooleanColumn() throws SQLException {
        try (Connection connection = createConnection();
             PreparedStatement preparedStatement = connection.prepareStatement("INSERT into boolean_table(id, value) values(?,?);");
             Statement verificationStatement = connection.createStatement()) {

            preparedStatement.setInt(1, 1);
            preparedStatement.setString(2, "0");
            preparedStatement.addBatch();

            // even using set object
            preparedStatement.setInt(1, 2);
            preparedStatement.setObject(2, "0");
            preparedStatement.addBatch();

            // false, f or any case of true is considered false (FALSE, falSE, False, etc)
            preparedStatement.setInt(1, 3);
            preparedStatement.setString(2, "false");
            preparedStatement.addBatch();

            preparedStatement.setInt(1, 4);
            preparedStatement.setString(2, "FALSE");
            preparedStatement.addBatch();

            // even with set object should work
            preparedStatement.setInt(1, 5);
            preparedStatement.setObject(2, "False");
            preparedStatement.addBatch();

            // smaller case f
            preparedStatement.setInt(1, 6);
            preparedStatement.setString(2, "f");
            preparedStatement.addBatch();

            // upper case F
            preparedStatement.setInt(1, 7);
            preparedStatement.setString(2, "F");
            preparedStatement.addBatch();


            preparedStatement.setInt(1, 8);
            preparedStatement.setString(2, "0.000");
            preparedStatement.addBatch();

            preparedStatement.setInt(1, 9);
            preparedStatement.setBoolean(2, false);
            preparedStatement.addBatch();

            preparedStatement.executeBatch();

            ResultSet resultSet = verificationStatement.executeQuery("select * from boolean_table");
            int actualCount = 0;
            while (resultSet.next()) {
                actualCount++;
                assertFalse(resultSet.getBoolean(2));
            }

            assertEquals(9, actualCount);
        }
    }

    @Test
    void willFailToSetTheBooleanValue() throws SQLException {
        try (Connection connection = createConnection();
             PreparedStatement preparedStatement = connection.prepareStatement("INSERT into boolean_table(id, value) values(?,?);");
             Statement verificationStatement = connection.createStatement()) {

            // int value is of 0 or 1 cannot be converted to boolean
            preparedStatement.setInt(1, 1);
            preparedStatement.setInt(2, 0);
            assertThrows(FireboltException.class, preparedStatement::execute);
            preparedStatement.clearParameters();

            preparedStatement.setInt(1, 1);
            preparedStatement.setInt(2, 1);
            assertThrows(FireboltException.class, preparedStatement::execute);
            preparedStatement.clearParameters();

            // int value is of 0 or 1 cannot be converted to boolean
            preparedStatement.setInt(1, 1);
            preparedStatement.setDouble(2, 0.0d);
            assertThrows(FireboltException.class, preparedStatement::execute);
            preparedStatement.clearParameters();

            preparedStatement.setInt(1, 1);
            preparedStatement.setDouble(2, 1.0d);
            assertThrows(FireboltException.class, preparedStatement::execute);
            preparedStatement.clearParameters();


            // any string other than true or false is not allowed
            preparedStatement.setInt(1, 1);
            preparedStatement.setString(2, "not_true_not_false");
            assertThrows(FireboltException.class, preparedStatement::execute);
            preparedStatement.clearParameters();

            preparedStatement.setInt(1, 1);
            preparedStatement.setObject(2, "not_true_not_false");
            assertThrows(FireboltException.class, preparedStatement::execute);
            preparedStatement.clearParameters();

            // no records should have been inserted
            ResultSet resultSet = verificationStatement.executeQuery("select * from boolean_table");
            assertFalse(resultSet.next());
        }
    }
}
