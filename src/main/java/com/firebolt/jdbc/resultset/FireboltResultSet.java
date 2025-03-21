package com.firebolt.jdbc.resultset;

import com.firebolt.jdbc.JdbcBase;
import com.firebolt.jdbc.QueryResult;
import com.firebolt.jdbc.annotation.ExcludeFromJacocoGeneratedReport;
import com.firebolt.jdbc.annotation.NotImplemented;
import com.firebolt.jdbc.exception.ExceptionType;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.exception.FireboltSQLFeatureNotSupportedException;
import com.firebolt.jdbc.resultset.column.Column;
import com.firebolt.jdbc.resultset.column.ColumnType;
import com.firebolt.jdbc.resultset.compress.LZ4InputStream;
import com.firebolt.jdbc.statement.FireboltStatement;
import com.firebolt.jdbc.type.BaseType;
import com.firebolt.jdbc.type.FireboltDataType;
import com.firebolt.jdbc.type.array.FireboltArray;
import com.firebolt.jdbc.type.array.SqlArrayUtil;
import com.firebolt.jdbc.type.lob.FireboltBlob;
import com.firebolt.jdbc.type.lob.FireboltClob;
import com.firebolt.jdbc.util.LoggerUtil;
import io.nats.jparse.Json;
import io.nats.jparse.node.Node;
import io.nats.jparse.node.RootNode;
import lombok.CustomLog;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.firebolt.jdbc.type.BaseType.isNull;
import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Optional.ofNullable;

/**
 * ResultSet for InputStream using the format "TabSeparatedWithNamesAndTypes"
 */
@CustomLog
public class FireboltResultSet extends JdbcBase implements ResultSet {
	private static final String FORWARD_ONLY_ERROR = "Cannot call %s() for ResultSet of type TYPE_FORWARD_ONLY";
	private static final int DEFAULT_CHAR_BUFFER_SIZE = 8192; // the default of BufferedReader
	private final BufferedReader reader;
	private final Map<String, Integer> columnNameToColumnNumber;
	private final FireboltResultSetMetaData resultSetMetaData;
	private final FireboltStatement statement;
	private final List<Column> columns;
	private final int maxRows;
	private final int maxFieldSize;
//	private Node currentResult;
	private JSONArray currentResult;
//	private Node currentJSON;
//	private List<Node> currentJSON;
	private JSONArray currentJSON;
	private int currentJSONLength = 0;
//	private FileWriter fileWriter;
	private int currentIndexFromJSON;
	private int currentRow = 1;
	private long totalBytesRead = 0;
	private int lastSplitRow = -1;
	private boolean isClosed = false;
	private long totalJSONParseTime = 0;
	private long totalReaderTime = 0;
	private long totalJsonObjectParseTime = 0;
	private long totalJsons = 0;
	private String[] arr = new String[0];

	private String lastReadValue = null;

	public FireboltResultSet(InputStream is, String tableName, String dbName, int bufferSize, boolean isCompressed,
							 FireboltStatement statement, boolean logResultSet) throws SQLException {
		log.debug("Creating resultSet...");
		this.statement = statement;
		if (logResultSet) {
//			is = LoggerUtil.logInputStream(is);
		}

		this.reader = createStreamReader(is, bufferSize, isCompressed);
		if (statement == null) {
			this.maxRows = 0;
			this.maxFieldSize = 0;
		} else {
			this.maxRows = statement.getMaxRows();
			this.maxFieldSize = statement.getMaxFieldSize();
		}

		try {
			String metadataJson = this.reader.readLine();
			columns = !(metadataJson == null || metadataJson.isEmpty())
					? getColumns(new JSONObject(metadataJson))
					: new ArrayList<>();
			this.columnNameToColumnNumber = new TreeMap<>(CASE_INSENSITIVE_ORDER);
			IntStream.range(0, columns.size()).boxed()
				.forEach(i -> columnNameToColumnNumber.put(columns.get(i).getColumnName(), i + 1));
//			if (columns.size() > 1) {
//				fileWriter = new FileWriter("result10gb.txt");
//				fileWriter.write(metadataJson);
//				fileWriter.append("\n");
//			}
			resultSetMetaData = new FireboltResultSetMetaData(dbName, tableName, columns);
		} catch (Exception e) {
			log.error("Could not create ResultSet: {}", e.getMessage(), e);
			throw new FireboltException("Cannot read response from DB: error while creating ResultSet ", e);
		}
		log.debug("ResultSet created");
	}

	public static FireboltResultSet of(QueryResult queryResult) throws SQLException {
		return new FireboltResultSet(new ByteArrayInputStream(queryResult.toString().getBytes()),
				queryResult.getTableName(), queryResult.getDatabaseName(), DEFAULT_CHAR_BUFFER_SIZE, false, null, false);

	}

	private BufferedReader createStreamReader(InputStream is, int bufferSize, boolean isCompressed) {
		InputStreamReader inputStreamReader;
		if (isCompressed) {
			inputStreamReader = new InputStreamReader(new LZ4InputStream(is), UTF_8);
		} else {
			inputStreamReader = new InputStreamReader(is, UTF_8);
		}
		return new BufferedReader(inputStreamReader, bufferSize);
	}

	@Override
	public boolean next() throws SQLException {
		checkStreamNotClosed();

		if (maxRows > 0 && currentRow > maxRows) {
			// if maxRows is configured (>0) and currentRow (minus 2 that is header lines) arrived >= maxRows
			// we are going to read the next line after maxRows, so return false to prevent it.
			return false;
		}

		try {
//			String currString = reader.readLine();
//			if (currString == null) {
//				return false;
//			}

			//checking index against length may work from the beginning so no need to check length == 0?
			if (currentJSON == null || currentJSONLength == currentIndexFromJSON) {
				long startTime = System.nanoTime();
				String json = reader.readLine();
//				fileWriter.append(json);
//				fileWriter.append("\n");
				totalBytesRead += json.length();
				long intermidiateTime = System.nanoTime();
				JSONObject currentRowObject = new JSONObject(json);
//				RootNode currentRowObject = Json.toRootNode(json);
//				RootNode currentRowObject = Json.toRootNode(reader.readLine());
				long endTime = System.nanoTime();
				totalJSONParseTime += (endTime - startTime);
				totalReaderTime += (intermidiateTime - startTime);
//				String messageType = currentRowObject.getNode("message_type").originalString();
				String messageType = currentRowObject.getString("message_type");
				if (messageType.equalsIgnoreCase("data")) {
//					currentJSON = currentRowObject.getNode("data");
//					currentJSON = new ArrayList<>(currentRowObject.getNode("data").asCollection().asArray());
					currentJSON = currentRowObject.getJSONArray("data");
					currentJSONLength = currentJSON.length();
//					currentJSONLength = currentJSON.size();
					currentIndexFromJSON = 0;
					totalJsons++;
				} else {
					log.info("Total time to read JSON: {}", TimeUnit.NANOSECONDS.toMillis(totalReaderTime));
					log.info("Total time to parse JSON: {}", TimeUnit.NANOSECONDS.toMillis(totalJSONParseTime));
					log.info("Total time to parse JSON object: {}", TimeUnit.NANOSECONDS.toMillis(totalJsonObjectParseTime));
					log.info("Total JSONs to parse: {}", totalJsons);
					log.info("Mean time to parse JSON: {}", TimeUnit.NANOSECONDS.toMillis(totalJSONParseTime / totalJsons));
					log.info("Total bytes read: {}", totalBytesRead);
//					fileWriter.close();
					return false;
				}
			}
			long startTime = System.nanoTime();
//			currentResult = currentJSON.get(currentIndexFromJSON);
//			currentResult = currentJSON.atPath(format("[%d]", currentIndexFromJSON));
			currentResult = currentJSON.getJSONArray(currentIndexFromJSON);
			long endTime = System.nanoTime();
			totalJsonObjectParseTime += (endTime - startTime);
			currentIndexFromJSON++;
			currentRow++;
		} catch (IOException e) {
			throw new SQLException("Error reading result from stream", e);
		}

		return true;
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		Column columnInfo = columns.get(columnIndex - 1);
		if (ofNullable(columnInfo).map(Column::getType).map(ColumnType::getDataType)
				.filter(t -> t.equals(FireboltDataType.BYTEA)).isPresent()) {
			// We do not need to escape when the type is BYTEA
			String hex = getValueAtColumn(columnIndex);
			if (isNull(hex)) {
				return null;
			}
			int maxHexStringSize = maxFieldSize * 2 + 2;
			if (maxFieldSize > 0 && maxHexStringSize <= hex.length()) {
				hex = hex.substring(0, maxHexStringSize);
			}
			return hex;
		} else {
			return BaseType.TEXT.transform(getValueAtColumn(columnIndex), null, null, maxFieldSize);
		}
	}

	@Override
	public String getString(String column) throws SQLException {
		return getString(findColumn(column));
	}

	@Override
	public int getInt(int columnIndex) throws SQLException {
		return getValue(columnIndex, BaseType.INTEGER, 0);
	}

	@Override
	public int getInt(String columnName) throws SQLException {
		return getInt(findColumn(columnName));
	}

	@Override
	public long getLong(int columnIndex) throws SQLException {
		return getValue(columnIndex, BaseType.LONG, 0L);
	}

	@Override
	public float getFloat(int columnIndex) throws SQLException {
		return getValue(columnIndex, BaseType.REAL, 0.0F);
	}

	@Override
	public float getFloat(String columnLabel) throws SQLException {
		return getFloat(findColumn(columnLabel));
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException {
		return getValue(columnIndex, BaseType.DOUBLE, 0.0);
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
		return getValue(columnIndex, BaseType.BYTE, (byte)0);
	}

	@Override
	public short getShort(int columnIndex) throws SQLException {
		return getValue(columnIndex, BaseType.SHORT, (short)0);
	}

	@Override
	public byte getByte(String column) throws SQLException {
		return getByte(findColumn(column));
	}

	@Override
	public short getShort(String columnLabel) throws SQLException {
		return getShort(findColumn(columnLabel));
	}

	private <T> T getValue(int columnIndex, BaseType type, T defaultValue) throws SQLException {
		T value = type.transform(getValueAtColumn(columnIndex));
		return value == null ? defaultValue : value;
	}

	@Override
	public byte[] getBytes(int colNum) throws SQLException {
		return ofNullable(getValueAtColumn(colNum))
				.map(v -> isNull(v) ? null : v)
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
//				if (fileWriter != null) {
//					fileWriter.close();
//				}
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
		return getValue(columnIndex, BaseType.NUMERIC, null);
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		return getBigDecimal(findColumn(columnLabel));
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
		return Optional.ofNullable(getBigDecimal(columnIndex)).map(d -> d.setScale(scale, RoundingMode.HALF_UP)).orElse(null);
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
		return getBigDecimal(findColumn(columnLabel), scale);
	}

	@Override
	public Array getArray(int columnIndex) throws SQLException {
		return BaseType.ARRAY.transform(getValueAtColumn(columnIndex), resultSetMetaData.getColumn(columnIndex));
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
		return getDateTime(columnIndex, calendar, BaseType.DATE);
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
		return getDateTime(columnIndex, calendar, BaseType.TIMESTAMP);
	}

	@Override
	public Timestamp getTimestamp(String columnLabel, Calendar calendar) throws SQLException {
		return getTimestamp(findColumn(columnLabel), calendar);
	}

	@Override
	public Time getTime(int columnIndex, Calendar calendar) throws SQLException {
		return getDateTime(columnIndex, calendar, BaseType.TIME);
	}

	private <T extends java.util.Date> T getDateTime(int columnIndex, Calendar calendar, BaseType type) throws SQLException {
		TimeZone timeZone = calendar != null ? calendar.getTimeZone() : null;
		String value = getValueAtColumn(columnIndex);
		return type.transform(value, resultSetMetaData.getColumn(columnIndex), timeZone, 0);
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
		return currentRow;
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
		if (isNull(value)) {
			return null;
		}
		Column columnInfo = columns.get(columnIndex - 1);
		FireboltDataType columnType = columnInfo.getType().getDataType();
		Object object = columnType.getBaseType().transform(value, columnInfo, columnInfo.getType().getTimeZone(), maxFieldSize);
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
	@NotImplemented
	public boolean isBeforeFirst() throws SQLException {
		throw new FireboltException(format(FORWARD_ONLY_ERROR, "isBeforeFirst"));
	}

	@Override
	@NotImplemented
	public boolean isAfterLast() throws SQLException {
		throw new FireboltException(format(FORWARD_ONLY_ERROR, "isAfterLast"));
	}

	@Override
	@NotImplemented
	public boolean isFirst() throws SQLException {
		throw new FireboltException(format(FORWARD_ONLY_ERROR, "isFirst"));
	}

	@Override
	@NotImplemented
	public boolean isLast() throws SQLException {
		throw new FireboltException(format(FORWARD_ONLY_ERROR, "isLast"));
	}

	@Override
	public boolean wasNull() throws SQLException {
		checkStreamNotClosed();
		if (lastReadValue == null) {
			throw new IllegalArgumentException("A column must be read before checking nullability");
		}
		return isNull(lastReadValue);
	}

	@Override
	@NotImplemented
	public boolean first() throws SQLException {
		throw new FireboltException(format(FORWARD_ONLY_ERROR, "first"));
	}

	@Override
	@NotImplemented
	public boolean last() throws SQLException {
		throw new FireboltException(format(FORWARD_ONLY_ERROR, "last"));
	}

	private List<Column> getColumns(JSONObject metadataObject) {
		JSONArray jsonArray = metadataObject.getJSONArray("result_columns");
		return IntStream.range(0, jsonArray.length())
				.mapToObj(jsonArray::getJSONObject)
				.map(jsonObject -> Column.of(jsonObject.getString("type"), jsonObject.getString("name")))
				.collect(Collectors.toList());
	}

	private String getValueAtColumn(int columnIndex) throws SQLException {
		checkStreamNotClosed();
		String value = currentResult.optString(getColumnIndex(columnIndex), null);
//		String value = currentResult.atPath(format("[%d]", getColumnIndex(columnIndex))).originalString();
//		String value = currentJSON.atPath(format("[%d][%d]", currentIndexFromJSON - 1, getColumnIndex(columnIndex))).originalString();
		lastReadValue = value;
		return value;
	}

	private int getColumnIndex(int colNum) throws SQLException {
		validateColumnNumber(colNum);
		return colNum - 1;
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
		return ofNullable(getBytes(columnIndex)).map(ByteArrayInputStream::new).orElse(null);
	}

	@Override
	public InputStream getAsciiStream(String columnLabel) throws SQLException {
		return getAsciiStream(findColumn(columnLabel));
	}

	@Override
	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		return getUnicodeStream(findColumn(columnLabel));
	}

	@Override
	public InputStream getBinaryStream(String columnLabel) throws SQLException {
		return getBinaryStream(findColumn(columnLabel));
	}

	@Override
	public String getCursorName() throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	public Reader getCharacterStream(int columnIndex) throws SQLException {
		return ofNullable(getUnicodeStream(columnIndex)).map(InputStreamReader::new).orElse(null);
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
	public int getFetchDirection() {
		return ResultSet.FETCH_FORWARD;
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		if (direction != ResultSet.FETCH_FORWARD) {
			throw new FireboltException(ExceptionType.TYPE_NOT_SUPPORTED);
		}
	}

	@Override
	@SuppressWarnings("SpellCheckingInspection")
	public int getFetchSize() {
		return 0; // fetch size is not supported; 0 means unlimited like in PostgreSQL and MySQL
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
	public int getConcurrency() {
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
	public Statement getStatement() {
		return statement;
	}

	@Override
	public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
		FireboltDataType dataType = resultSetMetaData.getColumn(columnIndex).getType().getDataType();
		Map<String, Class<?>> caseInsensitiveMap = new TreeMap<>(CASE_INSENSITIVE_ORDER);
		caseInsensitiveMap.putAll(map);
		Class<?> type = getAllNames(dataType).map(caseInsensitiveMap::get).filter(Objects::nonNull).findFirst()
				.orElseThrow(() -> new FireboltException(format("Cannot find type %s in provided types map", dataType)));
		return getObject(columnIndex, type);
	}

	private Stream<String> getAllNames(FireboltDataType dataType) {
		return Stream.concat(Stream.of(dataType.getDisplayName(), getJdbcType(dataType)).filter(Objects::nonNull), Stream.of(dataType.getAliases()));
	}

	private String getJdbcType(FireboltDataType dataType) {
		return JDBCType.valueOf(dataType.getSqlType()).getName();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public Ref getRef(int columnIndex) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	public Blob getBlob(int columnIndex) throws SQLException {
		return Optional.ofNullable(getBytes(columnIndex)).map(FireboltBlob::new).orElse(null);
	}

	@Override
	public Clob getClob(int columnIndex) throws SQLException {
		return Optional.ofNullable(getString(columnIndex)).map(String::toCharArray).map(FireboltClob::new).orElse(null);
	}

	@Override
	public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
		return getObject(findColumn(columnLabel), map);
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public Ref getRef(String columnLabel) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	public Blob getBlob(String columnLabel) throws SQLException {
		return getBlob(findColumn(columnLabel));
	}

	@Override
	public Clob getClob(String columnLabel) throws SQLException {
		return getClob(findColumn(columnLabel));
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
	public int getHoldability() {
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
	public NClob getNClob(int columnIndex) throws SQLException {
		String str = getString(columnIndex);
		return str == null ? null : new FireboltClob(str.toCharArray());
	}

	@Override
	public NClob getNClob(String columnLabel) throws SQLException {
		return getNClob(findColumn(columnLabel));
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
