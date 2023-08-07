package com.firebolt.jdbc.resultset;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.text.StringEscapeUtils;

import com.firebolt.jdbc.util.LoggerUtil;
import com.firebolt.jdbc.QueryResult;
import com.firebolt.jdbc.annotation.ExcludeFromJacocoGeneratedReport;
import com.firebolt.jdbc.annotation.NotImplemented;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.exception.FireboltSQLFeatureNotSupportedException;
import com.firebolt.jdbc.exception.FireboltUnsupportedOperationException;
import com.firebolt.jdbc.resultset.column.Column;
import com.firebolt.jdbc.resultset.column.ColumnType;
import com.firebolt.jdbc.resultset.compress.LZ4InputStream;
import com.firebolt.jdbc.statement.FireboltStatement;
import com.firebolt.jdbc.type.BaseType;
import com.firebolt.jdbc.type.FireboltDataType;
import com.firebolt.jdbc.type.array.FireboltArray;

import lombok.CustomLog;

/**
 * ResultSet for InputStream using the format "TabSeparatedWithNamesAndTypes"
 */
@CustomLog
public class FireboltResultSet implements ResultSet {
	private final BufferedReader reader;
	private final Map<String, Integer> columnNameToColumnNumber;
	private final FireboltResultSetMetaData resultSetMetaData;
	private final FireboltStatement statement;
	private final List<Column> columns;
	private final int maxRows;
	private String currentLine;
	private int currentRow = 0;
	private int lastSplitRow = -1;
	private boolean isClosed = false;
	private String[] arr = new String[0];

	private String lastReadValue = null;

	public FireboltResultSet(InputStream is, String tableName, String dbName) throws SQLException {
		this(is, tableName, dbName, null, false, null, false);
	}

	public FireboltResultSet(InputStream is) throws SQLException {
		this(is, null, null, null, false, null, false);
	}

	public FireboltResultSet(InputStream is, String tableName, String dbName, Integer bufferSize) throws SQLException {
		this(is, tableName, dbName, bufferSize, false, null, false);
	}

	public FireboltResultSet(InputStream is, String tableName, String dbName, Integer bufferSize,
			FireboltStatement fireboltStatement) throws SQLException {
		this(is, tableName, dbName, bufferSize, false, fireboltStatement, false);
	}

	private FireboltResultSet() {
		reader = // empty InputStream
				new BufferedReader(
						new InputStreamReader(new ByteArrayInputStream("".getBytes()), StandardCharsets.UTF_8));
		resultSetMetaData = FireboltResultSetMetaData.builder().columns(new ArrayList<>()).build();
		columnNameToColumnNumber = new HashMap<>();
		currentLine = null;
		columns = new ArrayList<>();
		statement = null;
		maxRows = 0;
	}

	public FireboltResultSet(InputStream is, String tableName, String dbName, Integer bufferSize, boolean isCompressed,
							 FireboltStatement statement, boolean logResultSet) throws SQLException {
		this(is, tableName, dbName, bufferSize, 0, isCompressed, statement, logResultSet);
	}

	public FireboltResultSet(InputStream is, String tableName, String dbName, Integer bufferSize, int maxRows, boolean isCompressed,
			FireboltStatement statement, boolean logResultSet) throws SQLException {
		log.debug("Creating resultSet...");
		this.statement = statement;
		if (logResultSet) {
			is = LoggerUtil.logInputStream(is);
		}

		this.reader = createStreamReader(is, bufferSize, isCompressed);
		this.maxRows = maxRows;

		try {
			this.next();
			String[] fields = toStringArray(currentLine);
			this.columnNameToColumnNumber = getColumnNamesToIndexes(fields);
			if (this.next()) {
				this.columns = getColumns(fields, currentLine);
			} else {
				this.columns = new ArrayList<>();
			}
			resultSetMetaData = FireboltResultSetMetaData.builder().columns(this.columns).tableName(tableName)
					.dbName(dbName).build();
		} catch (Exception e) {
			log.error("Could not create ResultSet: " + ExceptionUtils.getStackTrace(e), e);
			throw new FireboltException("Cannot read response from DB: error while creating ResultSet ", e);
		}
		log.debug("ResultSet created");
	}

	public static FireboltResultSet empty() {
		return new FireboltResultSet();
	}

	public static FireboltResultSet of(QueryResult queryResult) throws SQLException {
		return new FireboltResultSet(new ByteArrayInputStream(queryResult.toString().getBytes()),
				queryResult.getTableName(), queryResult.getDatabaseName());

	}

	private BufferedReader createStreamReader(InputStream is, Integer bufferSize, boolean isCompressed) {
		InputStreamReader inputStreamReader;
		if (isCompressed) {
			inputStreamReader = new InputStreamReader(new LZ4InputStream(is), StandardCharsets.UTF_8);
		} else {
			inputStreamReader = new InputStreamReader(is, StandardCharsets.UTF_8);
		}
		return bufferSize != null && bufferSize != 0 ? new BufferedReader(inputStreamReader, bufferSize)
				: new BufferedReader(inputStreamReader);
	}

	@Override
	public boolean next() throws SQLException {
		checkStreamNotClosed();

		if (maxRows > 0 && currentRow - 2 >= maxRows) {
			return false;
		}

		try {
			currentLine = reader.readLine();
			currentRow++;
		} catch (IOException e) {
			throw new SQLException("Error reading result from stream", e);
		}

		return currentLine != null;
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		Column columnInfo = this.columns.get(columnIndex - 1);
		if (Optional.ofNullable(columnInfo).map(Column::getType).map(ColumnType::getDataType)
				.filter(t -> t.equals(FireboltDataType.BYTEA)).isPresent()) {
			// We do not need to escape when the type is BYTEA
			return this.getValueAtColumn(columnIndex);
		} else {
			return BaseType.TEXT.transform(this.getValueAtColumn(columnIndex));
		}
	}

	@Override
	public String getString(String column) throws SQLException {
		return getString(findColumn(column));
	}

	@Override
	public int getInt(int columnIndex) throws SQLException {
		Integer value = BaseType.INTEGER.transform(getValueAtColumn(columnIndex));
		return value == null ? 0 : value;
	}

	@Override
	public int getInt(String columnName) throws SQLException {
		return this.getInt(findColumn(columnName));
	}

	@Override
	public long getLong(int colNum) throws SQLException {
		Long value = BaseType.LONG.transform(getValueAtColumn(colNum));
		return value == null ? 0 : value;
	}

	@Override
	public float getFloat(int columnIndex) throws SQLException {
		Float value = BaseType.REAL.transform(getValueAtColumn(columnIndex));
		return value == null ? 0 : value;
	}

	@Override
	public float getFloat(String columnLabel) throws SQLException {
		return this.getFloat(findColumn(columnLabel));
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException {
		Double value = BaseType.DOUBLE.transform(getValueAtColumn(columnIndex));
		return value == null ? 0 : value;
	}

	@Override
	public double getDouble(String columnLabel) throws SQLException {
		return this.getDouble(findColumn(columnLabel));
	}

	@Override
	public long getLong(String column) throws SQLException {
		return this.getLong(findColumn(column));
	}

	@Override
	public byte getByte(int columnIndex) throws SQLException {
		return Optional.ofNullable(getValueAtColumn(columnIndex)).map(v -> BaseType.isNull(v) ? null : v)
				.map(Byte::parseByte).orElse((byte) 0);
	}

	@Override
	public short getShort(int columnIndex) throws SQLException {
		Short value = BaseType.SHORT.transform(getValueAtColumn(columnIndex));
		return value == null ? 0 : value;
	}

	@Override
	public byte getByte(String column) throws SQLException {
		return this.getByte(findColumn(column));
	}

	@Override
	public short getShort(String columnLabel) throws SQLException {
		return this.getShort(findColumn(columnLabel));
	}

	@Override
	public byte[] getBytes(int colNum) throws SQLException {
		return Optional.ofNullable(getValueAtColumn(colNum)).map(v -> BaseType.isNull(v) ? null : v)
				.map(String::getBytes).orElse(null);
	}

	@Override
	public byte[] getBytes(String column) throws SQLException {
		return this.getBytes(findColumn(column));
	}

	@Override
	public synchronized void close() throws SQLException {
		if (!this.isClosed) {
			try {
				this.reader.close();
				this.isClosed = true;
			} catch (IOException e) {
				throw new SQLException("Could not close data stream when closing ResultSet", e);
			} finally {
				if (this.statement != null
						&& (this.statement.isCloseOnCompletion() && !this.statement.hasMoreResults())) {
					this.statement.close();
				}
			}
		}
	}

	@Override
	public int getType() {
		return TYPE_FORWARD_ONLY;
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		String value = getValueAtColumn(columnIndex);
		if (StringUtils.isEmpty(value)) {
			return null;
		}
		return BaseType.NUMERIC.transform(value);
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		return getBigDecimal(this.findColumn(columnLabel));
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
		BigDecimal bigDecimal = this.getBigDecimal(columnIndex);
		return bigDecimal == null ? null : bigDecimal.setScale(scale, RoundingMode.HALF_UP);
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
		return getBigDecimal(this.findColumn(columnLabel), scale);
	}

	@Override
	public Array getArray(int columnIndex) throws SQLException {
		String value = getValueAtColumn(columnIndex);
		return BaseType.ARRAY.transform(value, this.resultSetMetaData.getColumn(columnIndex));
	}

	@Override
	public Array getArray(String column) throws SQLException {
		return this.getArray(this.findColumn(column));
	}

	@Override
	public boolean getBoolean(String columnLabel) throws SQLException {
		return getBoolean(this.findColumn(columnLabel));
	}

	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {
		Boolean value = BaseType.BOOLEAN.transform(this.getValueAtColumn(columnIndex), this.resultSetMetaData.getColumn(columnIndex));
		return value != null && value;
	}

	@Override
	public Date getDate(String columnLabel) throws SQLException {
		return getDate(this.findColumn(columnLabel));
	}

	@Override
	public Date getDate(int columnIndex) throws SQLException {
		return getDate(columnIndex, null);
	}

	@Override
	public Date getDate(int columnIndex, Calendar calendar) throws SQLException {
		TimeZone timeZone = calendar != null ? calendar.getTimeZone() : null;
		String value = this.getValueAtColumn(columnIndex);
		return BaseType.DATE.transform(value, this.resultSetMetaData.getColumn(columnIndex), timeZone);
	}

	@Override
	public Date getDate(String columnLabel, Calendar cal) throws SQLException {
		return getDate(this.findColumn(columnLabel), cal);
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		return this.getTimestamp(columnIndex, null);
	}

	@Override
	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		return getTimestamp(this.findColumn(columnLabel));
	}

	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar calendar) throws SQLException {
		TimeZone timeZone = calendar != null ? calendar.getTimeZone() : null;
		String value = this.getValueAtColumn(columnIndex);
		return BaseType.TIMESTAMP.transform(value, this.resultSetMetaData.getColumn(columnIndex), timeZone);
	}

	@Override
	public Timestamp getTimestamp(String columnLabel, Calendar calendar) throws SQLException {
		return getTimestamp(this.findColumn(columnLabel), calendar);
	}

	@Override
	public Time getTime(int columnIndex, Calendar calendar) throws SQLException {
		TimeZone timeZone = calendar != null ? calendar.getTimeZone() : null;
		String value = this.getValueAtColumn(columnIndex);
		return BaseType.TIME.transform(value, this.resultSetMetaData.getColumn(columnIndex), timeZone);
	}

	@Override
	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
		if (type == null) {
			throw new FireboltException("The type provided is null");
		}
		String value = this.getValueAtColumn(columnIndex);
		Column column = this.resultSetMetaData.getColumn(columnIndex);
		BaseType columnType = column.getType().getDataType().getBaseType();
		return FieldTypeConverter.convert(type, value, columnType, column);
	}

	@Override
	public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
		return getObject(this.findColumn(columnLabel), type);
	}

	@Override
	public Time getTime(String columnLabel) throws SQLException {
		return getTime(this.findColumn(columnLabel));
	}

	@Override
	public Time getTime(int columnIndex) throws SQLException {
		return this.getTime(columnIndex, null);
	}

	@Override
	public Time getTime(String columnLabel, Calendar cal) throws SQLException {
		return getTime(this.findColumn(columnLabel), cal);
	}

	@Override
	public int getRow() {
		return currentRow;
	}

	@Override
	public boolean isClosed() {
		return isClosed;
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		return this.resultSetMetaData;
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		String value = getValueAtColumn(columnIndex);
		if (BaseType.isNull(value)) {
			return null;
		}
		Column columnInfo = this.columns.get(columnIndex - 1);
		FireboltDataType columnType = columnInfo.getType().getDataType();
		Object object = columnType.getBaseType().transform(value, columnInfo, columnInfo.getType().getTimeZone());
		if (columnType == FireboltDataType.ARRAY && object != null) {
			return ((FireboltArray) object).getArray();
		} else {
			return object;
		}
	}

	@Override
	public Object getObject(String column) throws SQLException {
		return getObject(findColumn(column));
	}

	@Override
	public boolean isBeforeFirst() throws SQLException {
		checkStreamNotClosed();
		return currentRow < 3 || !hasNext();
	}

	@Override
	public boolean isAfterLast() throws SQLException {
		return !hasNext() && currentLine == null;
	}

	private boolean hasNext() {
		return reader.lines().iterator().hasNext();
	}

	@Override
	public boolean isFirst() throws SQLException {
		checkStreamNotClosed();
		return currentRow == 3;
	}

	@Override
	public boolean isLast() throws SQLException {
		return !hasNext() && currentLine != null;
	}

	@Override
	public boolean wasNull() throws SQLException {
		checkStreamNotClosed();
		if (lastReadValue == null) {
			throw new IllegalArgumentException("A column must be read before checking nullability");
		}
		return BaseType.isNull(lastReadValue);
	}

	@Override
	public boolean first() throws SQLException {
		throw new FireboltException("Cannot call first() for ResultSet of type TYPE_FORWARD_ONLY");
	}

	@Override
	public boolean last() throws SQLException {
		throw new FireboltException("Cannot call last() for ResultSet of type TYPE_FORWARD_ONLY");
	}

	private String[] toStringArray(String stringToSplit) {
		if (currentRow != lastSplitRow) {
			if (StringUtils.isNotEmpty(stringToSplit)) {
				arr = StringUtils.splitPreserveAllTokens(stringToSplit, '\t');
			} else if (StringUtils.equals(stringToSplit, "")) {
				arr = new String[] { "" };
			} else {
				arr = new String[0];
			}
			lastSplitRow = currentRow;
		}
		return arr;
	}

	private List<Column> getColumns(String[] columnNames, String columnTypes) {
		String[] types = toStringArray(columnTypes);
		return IntStream.range(0, types.length)
				.mapToObj(i -> Column.of(types[i], StringEscapeUtils.unescapeJava(columnNames[i])))
				.collect(Collectors.toList());
	}

	private String getValueAtColumn(int columnIndex) throws SQLException {
		checkStreamNotClosed();
		String value = toStringArray(currentLine)[getColumnIndex(columnIndex)];
		lastReadValue = value;
		return value;
	}

	private int getColumnIndex(int colNum) throws SQLException {
		validateColumnNumber(colNum);
		return colNum - 1;
	}

	private Map<String, Integer> getColumnNamesToIndexes(String[] fields) {
		Map<String, Integer> columnNameToFieldIndex = new HashMap<>();
		if (fields != null) {
			for (int i = 0; i < fields.length; i++) {
				columnNameToFieldIndex.put(fields[i], i + 1);
			}
		}
		return columnNameToFieldIndex;
	}

	private void checkStreamNotClosed() throws SQLException {
		if (isClosed()) {
			throw new SQLException("Cannot proceed: stream closed");
		}
	}

	private void validateColumnNumber(int columnNumber) throws SQLException {
		if (columnNumber > columns.size()) {
			throw new SQLException(
					String.format("There is no column with number %d. Total of of columns available: %d ", columnNumber,
							columns.size()));
		}
	}

	@Override
	public int findColumn(String columnName) throws SQLException {
		Integer index = columnNameToColumnNumber.get(columnName);
		if (index == null) {
			throw new SQLException(String.format("There is no column with name %s ", columnName));
		}
		return index;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) {
		return iface.isAssignableFrom(getClass());
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		if (iface.isAssignableFrom(getClass())) {
			return iface.cast(this);
		}
		throw new SQLException("Cannot unwrap to " + iface.getName());
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public InputStream getAsciiStream(String columnLabel) throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public InputStream getBinaryStream(String columnLabel) throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public SQLWarning getWarnings() throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void clearWarnings() throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public String getCursorName() throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public Reader getCharacterStream(int columnIndex) throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public Reader getCharacterStream(String columnLabel) throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void beforeFirst() throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void afterLast() throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public boolean absolute(int row) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public boolean relative(int rows) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public boolean previous() throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public int getFetchDirection() throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public int getFetchSize() throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		checkStreamNotClosed();
		if (rows < 0) {
			throw new FireboltException("The number of rows cannot be less than 0");
		}
		// Not supported
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public int getConcurrency() throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public boolean rowUpdated() throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public boolean rowInserted() throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public boolean rowDeleted() throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateNull(int columnIndex) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateByte(int columnIndex, byte x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateShort(int columnIndex, short x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateInt(int columnIndex, int x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateLong(int columnIndex, long x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateFloat(int columnIndex, float x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateDouble(int columnIndex, double x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateString(int columnIndex, String x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateDate(int columnIndex, Date x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateTime(int columnIndex, Time x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateObject(int columnIndex, Object x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateNull(String columnLabel) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateBoolean(String columnLabel, boolean x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateByte(String columnLabel, byte x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateShort(String columnLabel, short x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateInt(String columnLabel, int x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateLong(String columnLabel, long x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateFloat(String columnLabel, float x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateDouble(String columnLabel, double x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateString(String columnLabel, String x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateBytes(String columnLabel, byte[] x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateDate(String columnLabel, Date x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateTime(String columnLabel, Time x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateObject(String columnLabel, Object x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void insertRow() throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateRow() throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void deleteRow() throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void refreshRow() throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void cancelRowUpdates() throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void moveToInsertRow() throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void moveToCurrentRow() throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public Statement getStatement() throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public Ref getRef(int columnIndex) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public Blob getBlob(int columnIndex) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public Clob getClob(int columnIndex) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public Ref getRef(String columnLabel) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public Blob getBlob(String columnLabel) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public Clob getClob(String columnLabel) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public URL getURL(int columnIndex) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public URL getURL(String columnLabel) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateRef(int columnIndex, Ref x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateRef(String columnLabel, Ref x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateBlob(int columnIndex, Blob x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateBlob(String columnLabel, Blob x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateClob(int columnIndex, Clob x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateClob(String columnLabel, Clob x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateArray(int columnIndex, Array x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateArray(String columnLabel, Array x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public RowId getRowId(int columnIndex) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public RowId getRowId(String columnLabel) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateRowId(int columnIndex, RowId x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateRowId(String columnLabel, RowId x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public int getHoldability() throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateNString(int columnIndex, String nString) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateNString(String columnLabel, String nString) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public NClob getNClob(int columnIndex) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public NClob getNClob(String columnLabel) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public String getNString(int columnIndex) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public String getNString(String columnLabel) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateClob(int columnIndex, Reader reader) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateClob(String columnLabel, Reader reader) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateNClob(int columnIndex, Reader reader) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void updateNClob(String columnLabel, Reader reader) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}


}
