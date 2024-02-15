package integration.tests;

import integration.IntegrationTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SpecialValuesTest extends IntegrationTest {
    private Connection systemConnection;
    private Connection userConnection;

    @BeforeAll
    void beforeAll() throws SQLException {
        systemConnection = createConnection(getSystemEngineName());
        userConnection = createConnection();
    }

    @AfterAll
    void afterAll() throws SQLException {
        systemConnection.close();
        userConnection.close();
    }

    @Test
    void longLiteral() throws FileNotFoundException, SQLException {
        try (Statement statement = systemConnection.createStatement();  PrintWriter pw = new PrintWriter(new FileOutputStream("/tmp/myresult"))) {
            for (int i = 1; i < 1000_000; i*=2) {
                String columns = IntStream.range(0, i).boxed().map(j -> "'a'").collect(Collectors.joining(","));
                String query = "select " + columns + " from information_schema.users";
                pw.println(i);
                pw.flush();
                statement.executeQuery(query);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"select 'inf'::float", "select '+inf'::float"})
    void infFloatUserEngine(String query) throws SQLException {
        specialSelect(userConnection, query, Float.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Float.POSITIVE_INFINITY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select 'inf'::float", "select '+inf'::float", "select 'inf'::double", "select '+inf'::double"})
    void infSystemEngine(String query) throws SQLException {
        //metadata returns type double, so there is no way to recognize float even if float conversion was called
        specialSelect(systemConnection, query, Float.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select 'inf'::double", "select '+inf'::double"})
    void infDoubleUserEngine(String query) throws SQLException {
        specialSelect(userConnection, query, Float.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select '-inf'::float"})
    void minusInfFloatUserEngine(String query) throws SQLException {
        specialSelect(userConnection, query, Float.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select '-inf'::float"})
    void minusInfSystemEngine(String query) throws SQLException {
        //metadata returns type double, so there is no way to recognize float even if float conversion was called
        specialSelect(systemConnection, query, Float.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select '-inf'::double"})
    void minusInfDoubleUserEngine(String query) throws SQLException {
        specialSelect(userConnection, query, Float.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select 'nan'::float", "select '+nan'::float", "select '-nan'::float"})
    void nanFloatUserEngine(String query) throws SQLException {
        specialSelect(userConnection, query, Float.NaN, Double.NaN, Float.NaN);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "select 'nan'::float", "select '+nan'::float", "select '-nan'::float",
            "select 'nan'::double", "select '+nan'::double", "select '-nan'::double"
    })
    void nanSystemEngine(String query) throws SQLException {
        //metadata returns type double, so there is no way to recognize float even if float conversion was called
        specialSelect(systemConnection, query, Float.NaN, Double.NaN, Double.NaN);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select 'nan'::double", "select '+nan'::double", "select '-nan'::double"})
    void nanDoubleUserEngine(String query) throws SQLException {
        specialSelect(userConnection, query, Float.NaN, Double.NaN, Double.NaN);
    }

    private void specialSelect(Connection connection, String query, Number floatGetObjectValue, Number doubleGetObjectValue, Number expectedGetObjectValue) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(query)) {
            resultSet.next();
            assertThrows(IllegalArgumentException.class, () -> resultSet.getShort(1));
            assertThrows(IllegalArgumentException.class, () -> resultSet.getInt(1));
            assertThrows(IllegalArgumentException.class, () -> resultSet.getLong(1));
            assertEquals(floatGetObjectValue, resultSet.getFloat(1));
            assertEquals(doubleGetObjectValue, resultSet.getDouble(1));
            assertEquals(expectedGetObjectValue, resultSet.getObject(1));
        }
    }
}
