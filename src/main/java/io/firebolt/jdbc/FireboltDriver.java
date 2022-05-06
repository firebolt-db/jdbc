package io.firebolt.jdbc;

import io.firebolt.jdbc.connection.FireboltConnectionImpl;
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
    return new FireboltConnectionImpl(url, connectionSettings);
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    return StringUtils.isNotEmpty(url) && url.startsWith(JDBC_FIREBOLT_PREFIX);
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getMajorVersion() {
    return ProjectVersionUtil.getMajorVersion();
  }

  @Override
  public int getMinorVersion() {
    return ProjectVersionUtil.getMinorVersion();
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
