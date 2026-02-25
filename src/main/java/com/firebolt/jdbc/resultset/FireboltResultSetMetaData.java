package com.firebolt.jdbc.resultset;

import com.firebolt.jdbc.GenericWrapper;
import com.firebolt.jdbc.resultset.column.Column;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

import static java.lang.String.format;

@SuppressWarnings("java:S4144") // Methods should not have identical implementations - many methods here have trivial identical implementation
public class FireboltResultSetMetaData implements ResultSetMetaData, GenericWrapper {
	private final String dbName;
	private final String tableName;
	private final List<Column> columns;

	public FireboltResultSetMetaData(String dbName, String tableName, List<Column> columns) {
		this.dbName = dbName;
		this.tableName = tableName;
		this.columns = columns;
	}

	@Override
	public int getColumnCount() {
		return columns.size();
	}

	@Override
	public int isNullable(int column) throws SQLException {
		return getColumn(column).getType().isNullable() ? columnNullable : columnNoNulls;
	}

	@Override
	public boolean isSigned(int column) throws SQLException {
		return getColumn(column).getType().getDataType().isSigned();
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
		return getColumn(column).getType().getPrecision();
	}

	@Override
	public int getScale(int column) throws SQLException {
		return getColumn(column).getType().getScale();
	}

	@Override
	public String getTableName(int column) throws SQLException {
		checkColumnNumber(column);
		return tableName;
	}

	@Override
	public String getCatalogName(int column) throws SQLException {
		checkColumnNumber(column);
		return dbName;
	}

	@Override
	public int getColumnType(int column) throws SQLException {
		return getColumn(column).getType().getDataType().getSqlType();
	}

	@Override
	public String getColumnTypeName(int column) throws SQLException {
		return getColumn(column).getType().getCompactTypeName();
	}

    public String getFullyQualifiedColumnTypeName(int column) throws SQLException {
		return getColumn(column).getType().getName();
	}

	@Override
	public String getColumnClassName(int column) throws SQLException {
		return getColumn(column).getType().getDataType().getBaseType().getType().getName();
	}

	Column getColumn(int column) throws SQLException {
		checkColumnNumber(column);
		return columns.get(column - 1);
	}

	@Override
	public boolean isCaseSensitive(int column) throws SQLException {
		return getColumn(column).getType().getDataType().isCaseSensitive();
	}

	@Override
	public boolean isAutoIncrement(int column) throws SQLException {
		checkColumnNumber(column);
		return false;
	}

	@Override
	public boolean isSearchable(int column) throws SQLException {
		checkColumnNumber(column);
		return true;
	}

	@Override
	public boolean isCurrency(int column) throws SQLException {
		checkColumnNumber(column);
		return false;
	}

	@Override
	public int getColumnDisplaySize(int column) throws SQLException {
		checkColumnNumber(column);
		return 80; // Default value for backward compatibility
	}
	@Override
	public String getSchemaName(int column) throws SQLException {
		checkColumnNumber(column);
		return ""; // Schemas are not implemented so N/A
	}

	@Override
	public boolean isReadOnly(int column) throws SQLException {
		checkColumnNumber(column);
		return true;
	}

	@Override
	public boolean isWritable(int column) throws SQLException {
		return !isReadOnly(column);
	}

	@Override
	public boolean isDefinitelyWritable(int column) throws SQLException {
		return isWritable(column);
	}


	private void checkColumnNumber(int column) throws SQLException {
		if (column < 1 || column > columns.size()) {
			throw new SQLException(format("Invalid column number %d", column));
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		FireboltResultSetMetaData that = (FireboltResultSetMetaData) o;
		return Objects.equals(dbName, that.dbName) && Objects.equals(tableName, that.tableName) && Objects.equals(columns, that.columns);
	}

	@Override
	public int hashCode() {
		return Objects.hash(dbName, tableName, columns);
	}

	@Override
	public String toString() {
		return format("FireboltResultSetMetaData{dbName=%s tableName=%s, columns=%s}", dbName, tableName, columns);
	}
}
