package com.firebolt.jdbc.resultset;

import com.firebolt.jdbc.QueryResult;
import com.firebolt.jdbc.annotation.ExcludeFromJacocoGeneratedReport;
import com.firebolt.jdbc.annotation.NotImplemented;
import com.firebolt.jdbc.exception.ExceptionType;
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
import com.firebolt.jdbc.type.array.SqlArrayUtil;
import com.firebolt.jdbc.util.LoggerUtil;
import lombok.CustomLog;
import org.apache.commons.text.StringEscapeUtils;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.firebolt.jdbc.util.StringUtil.splitAll;
import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Optional.ofNullable;

/**
 * ResultSet for InputStream using the format "TabSeparatedWithNamesAndTypes"
 */
@CustomLog
public class FireboltResultSet implements ResultSet {
	private static final String FORWARD_ONLY_ERROR = "Cannot call %s() for ResultSet of type TYPE_FORWARD_ONLY";
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
		reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream("".getBytes()), UTF_8)); // empty InputStream
		resultSetMetaData = new FireboltResultSetMetaData(null, null, List.of());
		columnNameToColumnNumber = new TreeMap<>(CASE_INSENSITIVE_ORDER);
		currentLine = null;
		columns = new ArrayList<>();
		statement = null;
		maxRows = 0;
	}

	public FireboltResultSet(InputStream is, String tableName, String dbName, Integer bufferSize, boolean isCompressed,
							 FireboltStatement statement, boolean logResultSet) throws SQLException {
		this(is, tableName, dbName, bufferSize, 0, isCompressed, statement, logResultSet);
	}

	@SuppressWarnings("java:S107") //Number of parameters (8) > max (7). This is the price of the immutability
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
			next();
			String[] fields = toStringArray(currentLine);
			this.columnNameToColumnNumber = getColumnNamesToIndexes(fields);
			if (next()) {
				columns = getColumns(fields, currentLine);
			} else {
				columns = new ArrayList<>();
			}
			resultSetMetaData = new FireboltResultSetMetaData(dbName, tableName, columns);
		} catch (Exception e) {
			log.error("Could not create ResultSet: {}", e.getMessage(), e);
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
			inputStreamReader = new InputStreamReader(new LZ4InputStream(is), UTF_8);
		} else {
			inputStreamReader = new InputStreamReader(is, UTF_8);
		}
		return bufferSize != null && bufferSize != 0 ? new BufferedReader(inputStreamReader, bufferSize)
				: new BufferedReader(inputStreamReader);
	}

	@Override
	public boolean next() throws SQLException {
		checkStreamNotClosed();

		if (maxRows > 0 && currentRow - 2 >= maxRows) {
			// if maxRows is configured (>0) and currentRow (minus 2 that is header lines) arrived >= maxRows
			// we are going to read the next line after maxRows, so return false to prevent it.
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
		Column columnInfo = columns.get(columnIndex - 1);
		if (ofNullable(columnInfo).map(Column::getType).map(ColumnType::getDataType)
				.filter(t -> t.equals(FireboltDataType.BYTEA)).isPresent()) {
			// We do not need to escape when the type is BYTEA
			return getValueAtColumn(columnIndex);
		} else {
			return BaseType.TEXT.transform(getValueAtColumn(columnIndex));
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
		return getInt(findColumn(columnName));
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
		return getFloat(findColumn(columnLabel));
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException {
		Double value = BaseType.DOUBLE.transform(getValueAtColumn(columnIndex));
		return value == null ? 0 : value;
	}

	@Override
	public double getDouble(String columnLabel) throws SQLException {
		return getDouble(findColumn(columnLabel));
	}

	@Override
	public long getLong(String column) throws SQLException {
		return getLong(findColumn(column));
	}

	@Override
	public byte getByte(int columnIndex) throws SQLException {
		return ofNullable(getValueAtColumn(columnIndex)).map(v -> BaseType.isNull(v) ? null : v)
				.map(Byte::parseByte).orElse((byte) 0);
	}

	@Override
	public short getShort(int columnIndex) throws SQLException {
		Short value = BaseType.SHORT.transform(getValueAtColumn(columnIndex));
		return value == null ? 0 : value;
	}

	@Override
	public byte getByte(String column) throws SQLException {
		return getByte(findColumn(column));
	}

	@Override
	public short getShort(String columnLabel) throws SQLException {
		return getShort(findColumn(columnLabel));
	}

	@Override
	public byte[] getBytes(int colNum) throws SQLException {
		return ofNullable(getValueAtColumn(colNum))
				.map(v -> BaseType.isNull(v) ? null : v)
				.map(SqlArrayUtil::hexStringToByteArray)
				.orElse(null);
	}

	@Override
	public byte[] getBytes(String column) throws SQLException {
		return getBytes(findColumn(column));
	}

	@Override
	public synchronized void close() throws SQLException {
		if (!isClosed) {
			try {
				reader.close();
				isClosed = true;
			} catch (IOException e) {
				throw new SQLException("Could not close data stream when closing ResultSet", e);
			} finally {
				if (statement != null && (statement.isCloseOnCompletion() && !statement.hasMoreResults())) {
					statement.close();
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
		return value == null || value.isEmpty() ? null : BaseType.NUMERIC.transform(value);
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		return getBigDecimal(findColumn(columnLabel));
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
		BigDecimal bigDecimal = getBigDecimal(columnIndex);
		return bigDecimal == null ? null : bigDecimal.setScale(scale, RoundingMode.HALF_UP);
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
		return getBigDecimal(findColumn(columnLabel), scale);
	}

	@Override
	public Array getArray(int columnIndex) throws SQLException {
		String value = getValueAtColumn(columnIndex);
		return BaseType.ARRAY.transform(value, resultSetMetaData.getColumn(columnIndex));
	}

	@Override
	public Array getArray(String column) throws SQLException {
		return getArray(findColumn(column));
	}

	@Override
	public boolean getBoolean(String columnLabel) throws SQLException {
		return getBoolean(findColumn(columnLabel));
	}

	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {
		Boolean value = BaseType.BOOLEAN.transform(getValueAtColumn(columnIndex), resultSetMetaData.getColumn(columnIndex));
		return value != null && value;
	}

	@Override
	public Date getDate(String columnLabel) throws SQLException {
		return getDate(findColumn(columnLabel));
	}

	@Override
	public Date getDate(int columnIndex) throws SQLException {
		return getDate(columnIndex, null);
	}

	@Override
	public Date getDate(int columnIndex, Calendar calendar) throws SQLException {
		TimeZone timeZone = calendar != null ? calendar.getTimeZone() : null;
		String value = getValueAtColumn(columnIndex);
		return BaseType.DATE.transform(value, resultSetMetaData.getColumn(columnIndex), timeZone);
	}

	@Override
	public Date getDate(String columnLabel, Calendar cal) throws SQLException {
		return getDate(findColumn(columnLabel), cal);
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		return getTimestamp(columnIndex, null);
	}

	@Override
	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		return getTimestamp(findColumn(columnLabel));
	}

	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar calendar) throws SQLException {
		TimeZone timeZone = calendar != null ? calendar.getTimeZone() : null;
		String value = getValueAtColumn(columnIndex);
		return BaseType.TIMESTAMP.transform(value, resultSetMetaData.getColumn(columnIndex), timeZone);
	}

	@Override
	public Timestamp getTimestamp(String columnLabel, Calendar calendar) throws SQLException {
		return getTimestamp(findColumn(columnLabel), calendar);
	}

	@Override
	public Time getTime(int columnIndex, Calendar calendar) throws SQLException {
		TimeZone timeZone = calendar != null ? calendar.getTimeZone() : null;
		String value = getValueAtColumn(columnIndex);
		return BaseType.TIME.transform(value, resultSetMetaData.getColumn(columnIndex), timeZone);
	}

	@Override
	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
		if (type == null) {
			throw new FireboltException("The type provided is null");
		}
		String value = getValueAtColumn(columnIndex);
		Column column = resultSetMetaData.getColumn(columnIndex);
		BaseType columnType = column.getType().getDataType().getBaseType();
		return FieldTypeConverter.convert(type, value, columnType, column);
	}

	@Override
	public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
		return getObject(findColumn(columnLabel), type);
	}

	@Override
	public Time getTime(String columnLabel) throws SQLException {
		return getTime(findColumn(columnLabel));
	}

	@Override
	public Time getTime(int columnIndex) throws SQLException {
		return getTime(columnIndex, null);
	}

	@Override
	public Time getTime(String columnLabel, Calendar cal) throws SQLException {
		return getTime(findColumn(columnLabel), cal);
	}

	@Override
	public int getRow() {
		return currentRow - 2;
	}

	@Override
	public boolean isClosed() {
		return isClosed;
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		return resultSetMetaData;
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		String value = getValueAtColumn(columnIndex);
		if (BaseType.isNull(value)) {
			return null;
		}
		Column columnInfo = columns.get(columnIndex - 1);
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
		throw new FireboltException(format(FORWARD_ONLY_ERROR, "first"));
	}

	@Override
	public boolean last() throws SQLException {
		throw new FireboltException(format(FORWARD_ONLY_ERROR, "last"));
	}

	private String[] toStringArray(String stringToSplit) {
		if (currentRow != lastSplitRow) {
			arr = splitAll(stringToSplit, '\t');
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
		Map<String, Integer> columnNameToFieldIndex = new TreeMap<>(CASE_INSENSITIVE_ORDER);
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
					format("There is no column with number %d. Total of of columns available: %d ", columnNumber,
							columns.size()));
		}
	}

	@Override
	public int findColumn(String columnName) throws SQLException {
		Integer index = columnNameToColumnNumber.get(columnName);
		if (index == null) {
			throw new SQLException(format("There is no column with name %s ", columnName));
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
	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		return getTextStream(columnIndex, StandardCharsets.US_ASCII);
	}

	@Override
	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		return getTextStream(columnIndex, StandardCharsets.UTF_8);
	}

	private InputStream getTextStream(int columnIndex, Charset charset) throws SQLException {
		return ofNullable(getString(columnIndex)).map(str -> str.getBytes(charset)).map(ByteArrayInputStream::new).orElse(null);
	}

	@Override
	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		return ofNullable(getString(columnIndex)).map(String::getBytes).map(ByteArrayInputStream::new).orElse(null);
	}

	@Override
	public InputStream getAsciiStream(String columnLabel) throws SQLException {
		return getBinaryStream(columnLabel);
	}

	@Override
	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		return getBinaryStream(columnLabel);
	}

	@Override
	public InputStream getBinaryStream(String columnLabel) throws SQLException {
		return getBinaryStream(findColumn(columnLabel));
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
	public String getCursorName() throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	public Reader getCharacterStream(int columnIndex) throws SQLException {
		return ofNullable(getBinaryStream(columnIndex)).map(InputStreamReader::new).orElse(null);
	}

	@Override
	public Reader getCharacterStream(String columnLabel) throws SQLException {
		return getCharacterStream(findColumn(columnLabel));
	}

	@Override
	public void beforeFirst() throws SQLException {
		throw new FireboltException(format(FORWARD_ONLY_ERROR, "beforeFirst"));
	}

	@Override
	public void afterLast() throws SQLException {
		throw new FireboltException(format(FORWARD_ONLY_ERROR, "afterLast"));
	}

	@Override
	public boolean absolute(int row) throws SQLException {
		throw new FireboltException(format(FORWARD_ONLY_ERROR, "absolute"));
	}

	@Override
	@NotImplemented
	public boolean relative(int rows) throws SQLException {
		throw new FireboltException(format(FORWARD_ONLY_ERROR, "relative"));
	}

	@Override
	@NotImplemented
	public boolean previous() throws SQLException {
		throw new FireboltException(format(FORWARD_ONLY_ERROR, "previous"));
	}

	@Override
	public int getFetchDirection() throws SQLException {
		return ResultSet.FETCH_FORWARD;
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		if (direction != ResultSet.FETCH_FORWARD) {
			throw new FireboltException(ExceptionType.TYPE_NOT_SUPPORTED);
		}
	}

	@Override
	@NotImplemented
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
	public int getConcurrency() throws SQLException {
		return ResultSet.CONCUR_READ_ONLY;
	}

	@Override
	public boolean rowUpdated() throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	public boolean rowInserted() throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	public boolean rowDeleted() throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateNull(int columnIndex) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateByte(int columnIndex, byte x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateShort(int columnIndex, short x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateInt(int columnIndex, int x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateLong(int columnIndex, long x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateFloat(int columnIndex, float x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateDouble(int columnIndex, double x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateString(int columnIndex, String x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateDate(int columnIndex, Date x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateTime(int columnIndex, Time x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
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
	public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateObject(int columnIndex, Object x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateNull(String columnLabel) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateBoolean(String columnLabel, boolean x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateByte(String columnLabel, byte x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateShort(String columnLabel, short x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateInt(String columnLabel, int x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateLong(String columnLabel, long x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
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
	public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateString(String columnLabel, String x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateBytes(String columnLabel, byte[] x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateDate(String columnLabel, Date x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateTime(String columnLabel, Time x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
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
	public Statement getStatement() throws SQLException {
		return statement;
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
	public URL getURL(int columnIndex) throws SQLException {
		return createURL(getString(columnIndex));
    }

	@Override
	public URL getURL(String columnLabel) throws SQLException {
		return createURL(getString(columnLabel));
	}

	private URL createURL(String url) throws SQLException {
		try {
			return url == null ? null : new URL(url);
		} catch (MalformedURLException e) {
			throw new SQLException(e);
		}
	}

	@Override
	@NotImplemented
	public void updateRef(int columnIndex, Ref x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateRef(String columnLabel, Ref x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateBlob(int columnIndex, Blob x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateBlob(String columnLabel, Blob x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateClob(int columnIndex, Clob x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateClob(String columnLabel, Clob x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateArray(int columnIndex, Array x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
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
	public void updateRowId(int columnIndex, RowId x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateRowId(String columnLabel, RowId x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	public int getHoldability() throws SQLException {
		return ResultSet.HOLD_CURSORS_OVER_COMMIT;
	}

	@Override
	@NotImplemented
	public void updateNString(int columnIndex, String nString) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateNString(String columnLabel, String nString) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
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
	public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	public String getNString(int columnIndex) throws SQLException {
		return getString(columnIndex);
	}

	@Override
	public String getNString(String columnLabel) throws SQLException {
		return getString(columnLabel);
	}

	@Override
	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		return getCharacterStream(columnIndex);
	}

	@Override
	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		return getCharacterStream(columnLabel);
	}

	@Override
	@NotImplemented
	public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateClob(int columnIndex, Reader reader) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateClob(String columnLabel, Reader reader) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateNClob(int columnIndex, Reader reader) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void updateNClob(String columnLabel, Reader reader) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}
}
