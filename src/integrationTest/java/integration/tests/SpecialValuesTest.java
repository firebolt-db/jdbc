package integration.tests;

import integration.EnvironmentCondition;
import integration.IntegrationTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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

    @ParameterizedTest
    @ValueSource(strings = {"select 'inf'::float", "select '+inf'::float"})
    @EnvironmentCondition(value = "2", comparison = EnvironmentCondition.Comparison.LT)
    void infFloatUserEngine(String query) throws SQLException {
        specialSelect(userConnection, query, Float.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Float.POSITIVE_INFINITY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select 'inf'::real", "select '+inf'::real"})
    void infRealUserEngine(String query) throws SQLException {
        specialSelect(userConnection, query, Float.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Float.POSITIVE_INFINITY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select 'inf'::real", "select '+inf'::real"})
    void infRealSystemEngine(String query) throws SQLException {
        specialSelect(userConnection, query, Float.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Float.POSITIVE_INFINITY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select 'inf'::float", "select '+inf'::float"})
    @EnvironmentCondition(value = "2", comparison = EnvironmentCondition.Comparison.GE)
    void infFloatAsDoubleUserEngine(String query) throws SQLException {
        specialSelect(userConnection, query, Float.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select 'inf'::float", "select '+inf'::float"})
    @EnvironmentCondition(value = "2", comparison = EnvironmentCondition.Comparison.GE)
    void infFloatAsDoubleSystemEngine(String query) throws SQLException {
        specialSelect(systemConnection, query, Float.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select 'inf'::float", "select '+inf'::float"})
    @EnvironmentCondition(value = "2", comparison = EnvironmentCondition.Comparison.LT)
    void infFloatSystemEngine(String query) throws SQLException {
        specialSelect(systemConnection, query, Float.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Float.POSITIVE_INFINITY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select 'inf'::double", "select '+inf'::double"})
    void infDoubleSystemEngine(String query) throws SQLException {
        specialSelect(systemConnection, query, Float.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select 'inf'::double", "select '+inf'::double"})
    void infDoubleUserEngine(String query) throws SQLException {
        specialSelect(userConnection, query, Float.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select '-inf'::float"})
    @EnvironmentCondition(value = "2", comparison = EnvironmentCondition.Comparison.LT)
    void minusInfFloatUserEngine(String query) throws SQLException {
        specialSelect(userConnection, query, Float.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select '-inf'::float"})
    @EnvironmentCondition(value = "2", comparison = EnvironmentCondition.Comparison.GE)
    void minusInfFloatAsDoubleUserEngine(String query) throws SQLException {
        specialSelect(userConnection, query, Float.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select '-inf'::real"})
    void minusInfRealUserEngine(String query) throws SQLException {
        specialSelect(userConnection, query, Float.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select '-inf'::real"})
    void minusInfRealSystemEngine(String query) throws SQLException {
        specialSelect(systemConnection, query, Float.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select '-inf'::float"})
    @EnvironmentCondition(value = "2", comparison = EnvironmentCondition.Comparison.GE)
    void minusInfFloatAsDoubleSystemEngine(String query) throws SQLException {
        specialSelect(systemConnection, query, Float.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select '-inf'::float"})
    @EnvironmentCondition(value = "2", comparison = EnvironmentCondition.Comparison.LT)
    void minusInfSystemEngine(String query) throws SQLException {
        specialSelect(systemConnection, query, Float.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select '-inf'::double"})
    void minusInfDoubleUserEngine(String query) throws SQLException {
        specialSelect(userConnection, query, Float.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select 'nan'::float", "select '+nan'::float", "select '-nan'::float"})
    @EnvironmentCondition(value = "2", comparison = EnvironmentCondition.Comparison.LT)
    void nanFloatUserEngine(String query) throws SQLException {
        specialSelect(userConnection, query, Float.NaN, Double.NaN, Float.NaN);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select 'nan'::real", "select '+nan'::real", "select '-nan'::real"})
    void nanRealUserEngine(String query) throws SQLException {
        specialSelect(userConnection, query, Float.NaN, Double.NaN, Float.NaN);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select 'nan'::real", "select '+nan'::real", "select '-nan'::real"})
    void nanRealSystemEngine(String query) throws SQLException {
        specialSelect(systemConnection, query, Float.NaN, Double.NaN, Float.NaN);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select 'nan'::float", "select '+nan'::float", "select '-nan'::float"})
    @EnvironmentCondition(value = "2", comparison = EnvironmentCondition.Comparison.GE)
    void nanFloatAsDoubleUserEngine(String query) throws SQLException {
        specialSelect(userConnection, query, Float.NaN, Double.NaN, Double.NaN);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "select 'nan'::double", "select '+nan'::double", "select '-nan'::double"
    })
    void nanDoubleSystemEngine(String query) throws SQLException {
        specialSelect(systemConnection, query, Float.NaN, Double.NaN, Double.NaN);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "select 'nan'::float", "select '+nan'::float", "select '-nan'::float",
    })
    @EnvironmentCondition(value = "2", comparison = EnvironmentCondition.Comparison.GE)
    void nanFloatAsDoubleSystemEngine(String query) throws SQLException {
        specialSelect(systemConnection, query, Float.NaN, Double.NaN, Double.NaN);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "select 'nan'::float", "select '+nan'::float", "select '-nan'::float",
    })
    @EnvironmentCondition(value = "2", comparison = EnvironmentCondition.Comparison.LT)
    void nanFloatSystemEngine(String query) throws SQLException {
        specialSelect(systemConnection, query, Float.NaN, Double.NaN, Float.NaN);
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
