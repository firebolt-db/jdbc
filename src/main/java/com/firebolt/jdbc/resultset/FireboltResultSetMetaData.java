package com.firebolt.jdbc.resultset;

import com.firebolt.jdbc.exception.FireboltException;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;

import java.sql.SQLException;
import java.util.List;

@Value
@Builder
@EqualsAndHashCode(callSuper = false)
public class FireboltResultSetMetaData extends AbstractResultSetMetaData {

  List<FireboltColumn> columns;

  String tableName;

  String dbName;

  @Override
  public int getColumnCount() throws SQLException {
    return columns.size();
  }

  @Override
  public int isNullable(int column) throws SQLException {
    return getColumn(column).isNullable() ? columnNullable : columnNoNulls;
  }

  @Override
  public boolean isSigned(int column) throws SQLException {
    return getColumn(column).getDataType().isSigned();
  }

  @Override
  public String getColumnLabel(int column) throws SQLException {
    return getColumnName(column);
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    return getColumn(column).getColumnName();
  }

  @Override
  public int getPrecision(int column) throws SQLException {
    return getColumn(column).getPrecision();
  }

  @Override
  public int getScale(int column) throws SQLException {
    return getColumn(column).getScale();
  }

  @Override
  public String getTableName(int column) throws SQLException {
    return this.tableName;
  }

  @Override
  public String getCatalogName(int column) throws SQLException {
    return this.dbName;
  }

  @Override
  public int getColumnType(int column) throws SQLException {
    return getColumn(column).getDataType().getSqlType();
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException {
    return getColumn(column).getCompactTypeName();
  }

  @Override
  public String getColumnClassName(int column) throws SQLException {
    return getColumn(column).getDataType().getBaseType().getType().getCanonicalName();
  }

  public FireboltColumn getColumn(int column) {
    return this.columns.get(column - 1);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T unwrap(Class<T> interfaceName) throws SQLException {
    if (isWrapperFor(interfaceName)) {
      return (T) this;
    }
    throw new FireboltException("Unable unwrap to " + interfaceName);
  }

  @Override
  public boolean isWrapperFor(Class<?> interfaceName) throws SQLException {
    return interfaceName != null && interfaceName.isAssignableFrom(getClass());
  }
}
