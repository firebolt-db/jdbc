package integration.tests.datatype;

import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.testutils.TestTag;
import integration.IntegrationTest;
import io.qameta.allure.internal.shadowed.jackson.core.JsonProcessingException;
import io.qameta.allure.internal.shadowed.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

//@Tag(TestTag.CORE)  - we don't have a public docker image that has the support this feature. Enable it when we do
@Tag(TestTag.V2)
public class JsonTest extends IntegrationTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @BeforeEach
    void beforeEach() throws SQLException {
        // Create a test table for the large data insertion test
        try (Connection connection = createConnection()) {
            Statement statement = connection.createStatement();

            statement.execute("DROP TABLE IF EXISTS json_table");

            statement.execute("CREATE TABLE IF NOT EXISTS json_table (id INT, value JSON)");
        }
    }

    @AfterEach
    void afterEach() throws SQLException {
        // drop the table
        try (Connection connection = createConnection()) {
            connection.createStatement().execute("DROP TABLE IF EXISTS json_table");
        }
    }

    @Test
    void canReadMetadataForJsonColumn() throws SQLException {
        try (Connection connection = createConnection();
             Statement statement = connection.createStatement()) {

            ResultSet resultSet = statement.executeQuery("select * from json_table");
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

            // id column
            assertEquals(Types.INTEGER, resultSetMetaData.getColumnType(1));
            assertEquals("id", resultSetMetaData.getColumnName(1));
            assertEquals("integer", resultSetMetaData.getColumnTypeName(1));
            assertEquals("id", resultSetMetaData.getColumnLabel(1));
            assertEquals("java.lang.Integer", resultSetMetaData.getColumnClassName(1));
            assertEquals(0, resultSetMetaData.getScale(1));
            assertEquals(0, resultSetMetaData.getPrecision(1));

            // value column
            assertEquals(Types.OTHER, resultSetMetaData.getColumnType(2));
            assertEquals("value", resultSetMetaData.getColumnName(2));
            assertEquals("json", resultSetMetaData.getColumnTypeName(2));
            assertEquals("value", resultSetMetaData.getColumnLabel(2));
            assertEquals("java.lang.String", resultSetMetaData.getColumnClassName(2));
            assertEquals(0, resultSetMetaData.getScale(2));
            assertEquals(0, resultSetMetaData.getPrecision(2));
        }
    }

    @Test
    void canInsertNullIntoJsonColumn() throws SQLException {
        try (Connection connection = createConnection();
             Statement statement = connection.createStatement()) {

            PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO json_table(id, value) VALUES(?,?)");

            preparedStatement.setInt(1, 1);
            preparedStatement.setString(2, null);
            preparedStatement.execute();

            ResultSet resultSet = statement.executeQuery("select * from json_table order by id asc");

            assertTrue(resultSet.next());

            assertEquals(1, resultSet.getInt(1));
            assertNull(resultSet.getString(2));

            assertFalse(resultSet.next());
        }
    }

    @Test
    void cannotInsertIntIntoJsonColumn() throws SQLException {
        try (Connection connection = createConnection();
             Statement statement = connection.createStatement()) {

            PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO json_table(id, value) VALUES(?,?)");

            preparedStatement.setInt(1, 1);
            preparedStatement.setInt(2, 10);
            try {
                preparedStatement.execute();
                fail("Should not have to be able to insert an integer into a json column");
            } catch (FireboltException e) {
               assertEquals("Line 1, Column 44: integer can't be assigned to column value of the type json", e.getErrorMessageFromServer());
            }

            ResultSet resultSet = statement.executeQuery("select * from json_table order by id asc");
            assertFalse(resultSet.next());
        }
    }

    @Test
    void canInsertAndReadJsonDataType() throws SQLException {
        List<Pair<Integer, String>> tableRows = new LinkedList<>();
        tableRows.add(Pair.of(1, "null"));
        tableRows.add(Pair.of(2, "true"));
        tableRows.add(Pair.of(3, "false"));
        tableRows.add(Pair.of(4, "0"));
        tableRows.add(Pair.of(5, "128"));
        tableRows.add(Pair.of(6, "32768"));
        tableRows.add(Pair.of(7, "\"2147483648\""));  // big decimal is stored as string
        tableRows.add(Pair.of(8, "\"9223372036854775808\""));
        tableRows.add(Pair.of(9, "\"18446744073709551615\""));
        tableRows.add(Pair.of(10, "3.1415926"));
        tableRows.add(Pair.of(11, "\"Hello world!\""));
        tableRows.add(Pair.of(12, "\"Hello UTF-8! :fire:\""));
        tableRows.add(Pair.of(13, "[]"));
        tableRows.add(Pair.of(14, "[1,2,3,4]"));
        tableRows.add(Pair.of(15, "{}"));
        tableRows.add(Pair.of(16, "{\"a\":{\"b\":\"c\"}}"));
        tableRows.add(Pair.of(17, "[{}]"));
        tableRows.add(Pair.of(18, "[{\"a\":[\"b\",{\"c\":\"d\"}]},[\"e\",\"f\",{\"g\":[\"h\",\"i\"]}]]"));
        tableRows.add(Pair.of(19, "{\":fire:\":\":fire_extinguisher:\",\"nested\":{\":droplet:\":\"water\",\":ice_cube:\":\"ice\"}}"));

        try (Connection connection = createConnection();
             Statement statement = connection.createStatement()) {

            PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO json_table(id, value) VALUES(?,?)");

            for (Pair<Integer, String> row : tableRows) {
                preparedStatement.setInt(1, row.getLeft());
                preparedStatement.setString(2, row.getRight());
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();

            ResultSet resultSet = statement.executeQuery("select * from json_table order by id asc");
            int actualCount = 0;
            while (resultSet.next()) {
                Pair<Integer, String> expectedRow = tableRows.get(actualCount);
                assertEquals(expectedRow.getLeft(), resultSet.getInt(1));
                String actualJson = resultSet.getString(2);
                assertEquals(expectedRow.getRight(), actualJson);

                // also validate that we can deserialize the value into a proper json
                try {
                    OBJECT_MAPPER.readTree(actualJson);
                } catch (JsonProcessingException e) {
                    fail("Cannot deserialize result into proper json:" + actualJson);
                }

                actualCount++;
            }

            assertEquals(19, actualCount);
        }
    }
}
