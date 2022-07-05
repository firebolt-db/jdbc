package io.firebolt.jdbc.connection;

import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import static io.firebolt.jdbc.LoggerUtil.FEATURE_NOT_SUPPORTED_YET;

@Slf4j
public abstract class AbstractConnection implements Connection {

  @Override
  public String nativeSQL(String sql) throws SQLException {
    throw new UnsupportedOperationException(String.format(FEATURE_NOT_SUPPORTED_YET, new Throwable().getStackTrace()[0].getMethodName()));
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    return false;
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {}



  @Override
  public boolean isReadOnly() throws SQLException {
    return false;
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {}

  @Override
  public void setCatalog(String catalog) throws SQLException {
    throw new UnsupportedOperationException(String.format(FEATURE_NOT_SUPPORTED_YET, new Throwable().getStackTrace()[0].getMethodName()));
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    return Connection.TRANSACTION_NONE;
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    throw new UnsupportedOperationException(String.format(FEATURE_NOT_SUPPORTED_YET, new Throwable().getStackTrace()[0].getMethodName()));
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {}

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getHoldability() throws SQLException {
    return 0;
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {}

  @Override
  public Savepoint setSavepoint() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {}

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }


  @Override
  public CallableStatement prepareCall(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }


  @Override
  public Clob createClob() throws SQLException {
    return null;
  }

  @Override
  public Blob createBlob() throws SQLException {
    return null;
  }

  @Override
  public NClob createNClob() throws SQLException {
    return null;
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    return null;
  }



  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {}

  @Override
  public String getClientInfo(String name) throws SQLException {
    return null;
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    return null;
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {}

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    throw new UnsupportedOperationException(String.format(FEATURE_NOT_SUPPORTED_YET, new Throwable().getStackTrace()[0].getMethodName()));
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    return null;
  }

  @Override
  public void abort(Executor executor) throws SQLException {
    this.close();
  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    throw new UnsupportedOperationException(String.format(FEATURE_NOT_SUPPORTED_YET, new Throwable().getStackTrace()[0].getMethodName()));
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    return 0;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isAssignableFrom(getClass())) {
      return iface.cast(this);
    }
    throw new SQLException("Cannot unwrap to " + iface.getName());
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isAssignableFrom(getClass());
  }
}
