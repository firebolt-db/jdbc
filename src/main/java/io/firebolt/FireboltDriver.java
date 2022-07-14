package io.firebolt;

import io.firebolt.jdbc.VersionUtil;
import io.firebolt.jdbc.PropertyUtil;
import io.firebolt.jdbc.connection.FireboltConnection;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

@Slf4j
public class FireboltDriver implements Driver {

  public static final String JDBC_FIREBOLT = "jdbc:firebolt:";
  private static final String JDBC_FIREBOLT_PREFIX = JDBC_FIREBOLT + "//";

  static {
    try {
      java.sql.DriverManager.registerDriver(new FireboltDriver());
      log.info("Driver registered");
    } catch (SQLException ex) {
      throw new RuntimeException("Cannot register the driver");
    }
  }

  @Override
  public Connection connect(String url, Properties connectionSettings) throws SQLException {
    return new FireboltConnection(url, connectionSettings);
  }

  @Override
  public boolean acceptsURL(String url) {
    return StringUtils.isNotEmpty(url) && url.startsWith(JDBC_FIREBOLT_PREFIX);
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    return PropertyUtil.getPropertyInfo(url, info);
  }

  @Override
  public int getMajorVersion() {
    return VersionUtil.getMajorDriverVersion();
  }

  @Override
  public int getMinorVersion() {
    return VersionUtil.getDriverMinorVersion();
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException();
  }
}
