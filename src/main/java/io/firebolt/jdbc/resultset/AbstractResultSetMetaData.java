package io.firebolt.jdbc.resultset;

import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public abstract class AbstractResultSetMetaData implements ResultSetMetaData {

  @Override
  public boolean isAutoIncrement(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    return true;
  }

  @Override
  public boolean isSearchable(int column) throws SQLException {
    return true;
  }

  @Override
  public boolean isCurrency(int column) throws SQLException {
    return false;
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    return 80;
  }

  @Override
  public String getSchemaName(int column) throws SQLException {
    return StringUtils.EMPTY;
  }

  @Override
  public boolean isReadOnly(int column) throws SQLException {
    return true;
  }

  @Override
  public boolean isWritable(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException {
    return false;
  }
}
