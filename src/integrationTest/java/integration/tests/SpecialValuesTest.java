package integration.tests;

import com.firebolt.jdbc.testutils.TestTag;
import integration.IntegrationTest;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import lombok.CustomLog;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@CustomLog
public class SpecialValuesTest extends IntegrationTest {
    private Connection systemConnection;
    private Connection userConnection;

    @BeforeAll
    void beforeAll() throws SQLException {
        try {
            systemConnection = createConnection(getSystemEngineName());
        } catch (Exception e) {
            log.warn("Could not create system engine connection", e);
        }
        userConnection = createConnection();
    }

    @AfterAll
    void afterAll() throws SQLException {
        try {
            systemConnection.close();
        } catch (Exception e) {
            log.warn("Could not create system engine connection", e);
        }
        userConnection.close();
    }

    @ParameterizedTest
    @ValueSource(strings = {"select 'inf'::float", "select '+inf'::float"})
    @Tag(TestTag.V1)
    void infFloatUserEngine(String query) throws SQLException {
        specialSelect(userConnection, query, Float.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Float.POSITIVE_INFINITY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select 'inf'::real", "select '+inf'::real"})
    @Tag(TestTag.V2)
    @Tag(TestTag.V1)
    @Tag(TestTag.CORE)
    void infRealUserEngine(String query) throws SQLException {
        specialSelect(userConnection, query, Float.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Float.POSITIVE_INFINITY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select 'inf'::real", "select '+inf'::real"})
    @Tag(TestTag.V2)
    void infRealSystemEngine(String query) throws SQLException {
        specialSelect(userConnection, query, Float.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Float.POSITIVE_INFINITY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select 'inf'::float", "select '+inf'::float"})
    @Tag(TestTag.V2)
    @Tag(TestTag.CORE)
    void infFloatAsDoubleUserEngine(String query) throws SQLException {
        specialSelect(userConnection, query, Float.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select 'inf'::float", "select '+inf'::float"})
    @Tag(TestTag.V2)
    void infFloatAsDoubleSystemEngine(String query) throws SQLException {
        specialSelect(systemConnection, query, Float.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select 'inf'::double", "select '+inf'::double"})
    @Tag(TestTag.V2)
    void infDoubleSystemEngine(String query) throws SQLException {
        specialSelect(systemConnection, query, Float.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select 'inf'::double", "select '+inf'::double"})
    @Tag(TestTag.V2)
    @Tag(TestTag.V1)
    @Tag(TestTag.CORE)
    void infDoubleUserEngine(String query) throws SQLException {
        specialSelect(userConnection, query, Float.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select '-inf'::float"})
    @Tag(TestTag.V1)
    void minusInfFloatUserEngine(String query) throws SQLException {
        specialSelect(userConnection, query, Float.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select '-inf'::float"})
    @Tag(TestTag.V2)
    @Tag(TestTag.CORE)
    void minusInfFloatAsDoubleUserEngine(String query) throws SQLException {
        specialSelect(userConnection, query, Float.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select '-inf'::real"})
    @Tag(TestTag.V2)
    @Tag(TestTag.V1)
    @Tag(TestTag.CORE)
    void minusInfRealUserEngine(String query) throws SQLException {
        specialSelect(userConnection, query, Float.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select '-inf'::real"})
    @Tag(TestTag.V2)
    void minusInfRealSystemEngine(String query) throws SQLException {
        specialSelect(systemConnection, query, Float.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select '-inf'::float"})
    @Tag(TestTag.V2)
    void minusInfFloatAsDoubleSystemEngine(String query) throws SQLException {
        specialSelect(systemConnection, query, Float.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select '-inf'::double"})
    @Tag(TestTag.V2)
    @Tag(TestTag.V1)
    @Tag(TestTag.CORE)
    void minusInfDoubleUserEngine(String query) throws SQLException {
        specialSelect(userConnection, query, Float.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select 'nan'::float", "select '+nan'::float", "select '-nan'::float"})
    @Tag(TestTag.V1)
    void nanFloatUserEngine(String query) throws SQLException {
        specialSelect(userConnection, query, Float.NaN, Double.NaN, Float.NaN);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select 'nan'::real", "select '+nan'::real", "select '-nan'::real"})
    @Tag(TestTag.V2)
    @Tag(TestTag.V1)
    @Tag(TestTag.CORE)
    void nanRealUserEngine(String query) throws SQLException {
        specialSelect(userConnection, query, Float.NaN, Double.NaN, Float.NaN);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select 'nan'::real", "select '+nan'::real", "select '-nan'::real"})
    @Tag(TestTag.V2)
    void nanRealSystemEngine(String query) throws SQLException {
        specialSelect(systemConnection, query, Float.NaN, Double.NaN, Float.NaN);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select 'nan'::float", "select '+nan'::float", "select '-nan'::float"})
    @Tag(TestTag.V2)
    @Tag(TestTag.CORE)
    void nanFloatAsDoubleUserEngine(String query) throws SQLException {
        specialSelect(userConnection, query, Float.NaN, Double.NaN, Double.NaN);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "select 'nan'::double", "select '+nan'::double", "select '-nan'::double"
    })
    @Tag(TestTag.V2)
    void nanDoubleSystemEngine(String query) throws SQLException {
        specialSelect(systemConnection, query, Float.NaN, Double.NaN, Double.NaN);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "select 'nan'::float", "select '+nan'::float", "select '-nan'::float",
    })
    @Tag(TestTag.V2)
    void nanFloatAsDoubleSystemEngine(String query) throws SQLException {
        specialSelect(systemConnection, query, Float.NaN, Double.NaN, Double.NaN);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select 'nan'::double", "select '+nan'::double", "select '-nan'::double"})
    @Tag(TestTag.V2)
    @Tag(TestTag.V1)
    @Tag(TestTag.CORE)
    void nanDoubleUserEngine(String query) throws SQLException {
        specialSelect(userConnection, query, Float.NaN, Double.NaN, Double.NaN);
    }

    private void specialSelect(Connection connection, String query, Number floatGetObjectValue, Number doubleGetObjectValue, Number expectedGetObjectValue) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(query)) {
            resultSet.next();
            assertThrows(SQLException.class, () -> resultSet.getShort(1));
            assertThrows(SQLException.class, () -> resultSet.getInt(1));
            assertThrows(SQLException.class, () -> resultSet.getLong(1));
            assertEquals(floatGetObjectValue, resultSet.getFloat(1));
            assertEquals(doubleGetObjectValue, resultSet.getDouble(1));
            assertEquals(expectedGetObjectValue, resultSet.getObject(1));
        }
    }
}
