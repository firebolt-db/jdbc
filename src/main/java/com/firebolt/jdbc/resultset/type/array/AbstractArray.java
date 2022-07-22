package com.firebolt.jdbc.resultset.type.array;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;

public abstract class AbstractArray implements Array {

  @Override
  public Object getArray(long index, int count) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getResultSet(long index, int count) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }
}
