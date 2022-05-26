package io.firebolt.jdbc.resultset;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

@Value
@Builder
@EqualsAndHashCode
public class FireboltResultSetMetaData implements ResultSetMetaData {

  List<FireboltColumn> columns;

  String tableName;

  String dbName;

  @Override
  public int getColumnCount() throws SQLException {
    return columns.size();
  }

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
  public int isNullable(int column) throws SQLException {
    return getColumn(column).isNullable() ? columnNullable : columnNoNulls;
  }

  @Override
  public boolean isSigned(int column) throws SQLException {
    return getColumn(column).getDataType().isSigned();
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    return 80;
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
  public String getSchemaName(int column) throws SQLException {
    return StringUtils.EMPTY;
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

  @Override
  public String getColumnClassName(int column) throws SQLException {
    return getColumn(column).getDataType().getBaseType().getType().getCanonicalName();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (isWrapperFor(iface)) {
      return (T) this;
    }
    throw new SQLException("Unable to unwrap to " + iface);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface != null && iface.isAssignableFrom(getClass());
  }

  public FireboltColumn getColumn(int column) {
    return this.columns.get(column - 1);
  }
}
