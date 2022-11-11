package com.firebolt.jdbc.resultset;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.firebolt.jdbc.annotation.ExcludeFromJacocoGeneratedReport;
import com.firebolt.jdbc.annotation.NotImplemented;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.resultset.column.Column;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@Builder
@EqualsAndHashCode(callSuper = false)
public class FireboltResultSetMetaData implements ResultSetMetaData {

	List<Column> columns;
	String tableName;
	String dbName;

	@Override
	public int getColumnCount() throws SQLException {
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
		return this.tableName;
	}

	@Override
	public String getCatalogName(int column) throws SQLException {
		return this.dbName;
	}

	@Override
	public int getColumnType(int column) throws SQLException {
		return getColumn(column).getType().getDataType().getSqlType();
	}

	@Override
	public String getColumnTypeName(int column) throws SQLException {
		return getColumn(column).getType().getCompactTypeName();
	}

	@Override
	public String getColumnClassName(int column) throws SQLException {
		return getColumn(column).getType().getDataType().getBaseType().getType().getCanonicalName();
	}

	public Column getColumn(int column) {
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

	@Override
	public boolean isCaseSensitive(int column) throws SQLException {
		return getColumn(column).getType().getDataType().isCaseSensitive();
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	/**
	 * @hidden
	 */
	public boolean isAutoIncrement(int column) throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean isSearchable(int column) throws SQLException {
		return true;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean isCurrency(int column) throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public int getColumnDisplaySize(int column) throws SQLException {
		//Default value for backward compatibility
		return 80;
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public String getSchemaName(int column) throws SQLException {
		//Schemas are not implemented so N/A
		return StringUtils.EMPTY;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean isReadOnly(int column) throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean isWritable(int column) throws SQLException {
		return !isReadOnly(column);
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean isDefinitelyWritable(int column) throws SQLException {
		return false;
	}
}
