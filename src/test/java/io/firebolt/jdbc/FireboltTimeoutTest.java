package io.firebolt.jdbc;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Slf4j
class FireboltTimeoutTest {

  @Test
  void whenTestThenDoNotTimeout() {
    String password = System.getProperty("password").replace("\"", "");
    String user = System.getProperty("user").replace("\"", "");
    String api = System.getProperty("api");
    String db = System.getProperty("db");
    long startTime = System.nanoTime();
    long endTime = System.nanoTime();
    long timeElapsed = endTime - startTime;
    try {
      Class.forName("io.firebolt.FireboltDriver");
      Connection con =
          DriverManager.getConnection(
              "jdbc:firebolt://" + api + "/" + db + "?use_standard_sql=0&advanced_mode=1",
              user,
              password);
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
