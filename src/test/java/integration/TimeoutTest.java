package integration;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
class TimeoutTest {

    @Test
    void whenTestThenDoNotTimeout() {
        long startTime = System.nanoTime();
        long endTime = System.nanoTime();
        long timeElapsed = endTime - startTime;
        try {
            Class.forName("io.firebolt.FireboltDriver");
            Connection con =
                    DriverManager.getConnection(
                            "jdbc:firebolt://" + ConnectionInfo.getInstance().getApi() + "/" + ConnectionInfo.getInstance().getDatabase() + "?use_standard_sql=0&advanced_mode=1",
                            ConnectionInfo.getInstance().getUser(),
                            ConnectionInfo.getInstance().getPassword());
            Statement stmt = con.createStatement();
            stmt.executeQuery("SELECT sleepEachRow(1) from numbers(360)");
            assertTrue(true);
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
        log.info("Time elapsed: " + timeElapsed);
    }
}
