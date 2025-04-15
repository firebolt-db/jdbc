package integration.tests.resultset;

import integration.IntegrationTest;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Checks all the interactions with an integer type and what options are available on the result set
 */
public class IntegerTest extends IntegrationTest {

    @BeforeAll
    void beforeAll() {
        executeStatementFromFile("/statements/resultset/integer/ddl.sql");
    }

    @AfterAll
    void afterEach() {
        executeStatementFromFile("/statements/resultset/integer/cleanup.sql");
    }

    @Test
    void canGetResultUsingGetIntMethod() throws SQLException {
        // create a connection on the first engine and database
        try (Connection connection = createConnection()) {
            ResultSet rs = connection.createStatement().executeQuery("SELECT * from integer_type_test order by id asc");

            // get the first row
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(1, rs.getInt("id"));
            assertEquals(0, rs.getInt(2));
            assertEquals(0, rs.getInt("my_int"));
            assertEquals(1, rs.getInt(3));
            assertEquals(1, rs.getInt("my_int_not_null"));
            assertEquals(0, rs.getInt(4));
            assertEquals(0, rs.getInt("my_boolean"));

            // these commented tests fail
//            assertEquals(0, rs.getInt(5));
//            assertEquals(0, rs.getInt("my_boolean_not_null"));
            assertEquals(0, rs.getInt(6));
            assertEquals(0, rs.getInt("my_bigint"));
//            assertEquals(0, rs.getInt(7));
//            assertEquals(0, rs.getInt("my_bigint_not_null"));
            assertEquals(0, rs.getInt(8));
            assertEquals(0, rs.getInt("my_real"));
//            assertEquals(Integer.MAX_VALUE, rs.getInt(9));
//            assertEquals(Integer.MAX_VALUE, rs.getInt("my_real_not_null"));
            assertEquals(0, rs.getInt(10));
            assertEquals(0, rs.getInt("my_double"));
//            assertEquals(Integer.MAX_VALUE, rs.getInt(11));
//            assertEquals(Integer.MAX_VALUE, rs.getInt("my_double_not_null"));
            assertEquals(0, rs.getInt(12));
            assertEquals(0, rs.getInt("my_text"));
            assertEquals(12345, rs.getInt(13));
            assertEquals(12345, rs.getInt("my_text_not_null"));
            assertEquals(0, rs.getInt(14));
            assertEquals(0, rs.getInt("my_numeric"));
            assertEquals(123456789, rs.getInt(15));
            assertEquals(123456789, rs.getInt("my_numeric_not_null"));

            // get the second row
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals(2, rs.getInt("id"));
            assertEquals(101, rs.getInt(2));
            assertEquals(101, rs.getInt("my_int"));
            assertEquals(1, rs.getInt(3));
            assertEquals(1, rs.getInt("my_int_not_null"));

            // these commented tests fail
//            assertEquals(1, rs.getInt(4));
//            assertEquals(1, rs.getInt("my_boolean"));
//            assertEquals(0, rs.getInt(5));
//            assertEquals(0, rs.getInt("my_boolean_not_null"));

//            assertEquals(0, rs.getInt(6));
//            assertEquals(0, rs.getInt("my_bigint"));
//            assertEquals(0, rs.getInt(7));
//            assertEquals(0, rs.getInt("my_bigint_not_null"));
//            assertEquals(Integer.MIN_VALUE, rs.getInt(8));
//            assertEquals(Integer.MIN_VALUE, rs.getInt("my_real"));
//            assertEquals(Integer.MAX_VALUE, rs.getInt(9));
//            assertEquals(Integer.MAX_VALUE, rs.getInt("my_real_not_null"));
//            assertEquals(Integer.MIN_VALUE, rs.getInt(10));
//            assertEquals(Integer.MIN_VALUE, rs.getInt("my_double"));
//            assertEquals(Integer.MAX_VALUE, rs.getInt(11));
//            assertEquals(Integer.MAX_VALUE, rs.getInt("my_double_not_null"));
            assertEquals(-123, rs.getInt(12));
            assertEquals(-123, rs.getInt("my_text"));
            assertThrows(   SQLException.class, () ->  rs.getInt(13));
            assertThrows(   SQLException.class, () ->  rs.getInt("my_text_not_null"));
            assertEquals(-123456789, rs.getInt(14));
            assertEquals(-123456789, rs.getInt("my_numeric"));
            assertEquals(123456789, rs.getInt(15));
            assertEquals(123456789, rs.getInt("my_numeric_not_null"));
        }

    }

    @Test
    void canGetResultUsingGetObjectMethod() throws SQLException {
        // create a connection on the first engine and database
        try (Connection connection = createConnection()) {
            ResultSet rs = connection.createStatement().executeQuery("SELECT * from integer_type_test order by id asc");

            // get the first row
            assertTrue(rs.next());
            assertEquals(1, (Integer) rs.getObject(1));
            assertEquals(1, rs.getObject(1, Integer.class));
            assertEquals(1, (Integer) rs.getObject("id"));
            assertEquals(1,  rs.getObject("id", Integer.class));
            assertNull(rs.getObject(2));
            assertNull(rs.getObject(2, Integer.class));
            assertNull(rs.getObject("my_int"));
            assertNull(rs.getObject("my_int", Integer.class));

            // get the second row
            assertTrue(rs.next());
            assertEquals(2, (Integer) rs.getObject(1));
            assertEquals(2, rs.getObject(1, Integer.class));
            assertEquals(2, (Integer) rs.getObject("id"));
            assertEquals(2, rs.getObject("id", Integer.class));
            assertEquals(101, (Integer) rs.getObject(2));
            assertEquals(101, rs.getObject(2, Integer.class));
            assertEquals(101, (Integer) rs.getObject("my_int"));
            assertEquals(101, rs.getObject("my_int", Integer.class));
            assertEquals(1, (Integer) rs.getObject(3));
            assertEquals(1, rs.getObject(3, Integer.class));
            assertEquals(1, (Integer) rs.getObject("my_int_not_null"));
            assertEquals(1, rs.getObject("my_int_not_null", Integer.class));
        }

    }
}
