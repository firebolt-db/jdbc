package io.firebolt.jdbc.resultset;

import io.firebolt.jdbc.exception.FireboltException;
import io.firebolt.jdbc.resultset.compress.LZ4InputStream;
import io.firebolt.jdbc.resultset.type.BaseType;
import io.firebolt.jdbc.resultset.type.FireboltDataType;
import io.firebolt.jdbc.resultset.type.array.FireboltArray;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.text.StringEscapeUtils;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.firebolt.jdbc.LoggerUtil.FEATURE_NOT_SUPPORTED_YET;

@Slf4j
@EqualsAndHashCode(callSuper = true)
public class FireboltResultSet extends AbstractResultSet {
  private final BufferedReader reader;
  private final Map<String, Integer> columnNameToColumnNumber;
  private final List<FireboltColumn> columns;
  private final FireboltResultSetMetaData resultSetMetaData;
  boolean wasNull = false;
  private String currentLine;
  private int currentRow = 0;
  private int lastSplittedRow = -1;
  private boolean isClosed = false;
  private String[] arr = new String[0];

  private FireboltResultSet() {
    reader = // empty InputStream
        new BufferedReader(
            new InputStreamReader(new ByteArrayInputStream("".getBytes()), StandardCharsets.UTF_8));
    resultSetMetaData = FireboltResultSetMetaData.builder().build();
    columnNameToColumnNumber = new HashMap<>();
    currentLine = null;
    columns = new ArrayList<>();
  }

  public FireboltResultSet(InputStream is) throws SQLException {
    this(is, null, null, null, false);
  }

  public FireboltResultSet(InputStream is, String tableName, String dbName, Integer bufferSize)
      throws SQLException {
    this(is, tableName, dbName, bufferSize, false);
  }

  public FireboltResultSet(
      InputStream is, String tableName, String dbName, Integer bufferSize, boolean isCompressed)
      throws SQLException {
    log.debug("Creating resultSet...");
    //is = LoggerUtil.logInputStream(is);

    this.reader = createStreamReader(is, bufferSize, isCompressed);

    try {
      this.next();
      String[] fields = toStringArray(currentLine);
      this.columnNameToColumnNumber = getColumnNamesToIndexes(fields);
      if (this.next()) {
        this.columns = getColumns(fields, currentLine);
      } else {
        this.columns = new ArrayList<>();
      }
      resultSetMetaData =
          FireboltResultSetMetaData.builder()
              .columns(this.columns)
              .tableName(tableName)
              .dbName(dbName)
              .build();
      log.debug("ResultSetMetaData created");
    } catch (Exception e) {
      log.error("Could not create ResultSet: " + ExceptionUtils.getStackTrace(e), e);
      throw new FireboltException(
          "Cannot read response from DB: error while creating ResultSet ", e);
    }
    log.debug("ResultSet created");
  }

  private BufferedReader createStreamReader(InputStream is, Integer bufferSize, boolean isCompressed) {
    if (isCompressed) {
      return bufferSize != null
          ? new BufferedReader(
              new InputStreamReader(new LZ4InputStream(is), StandardCharsets.UTF_8), bufferSize)
          : new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
    } else {
      return bufferSize != null
          ? new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8), bufferSize)
          : new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
    }
  }

  public static FireboltResultSet empty() {
    return new FireboltResultSet();
  }

  @Override
  public boolean next() throws SQLException {
    checkStreamNotClosed();

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
    return BaseType.STRING.transform(this.getValueAtColumn(columnIndex));
  }

  @Override
  public String getString(String column) throws SQLException {
    return getString(getColumnIndex(column));
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    Integer value = BaseType.INTEGER.transform(getValueAtColumn(columnIndex));
    return value == null ? 0 : value;
  }

  @Override
  public int getInt(String columnName) throws SQLException {
    return this.getInt(getColumnIndex(columnName));
  }

  @Override
  public long getLong(int colNum) throws SQLException {
    Long value = BaseType.LONG.transform(getValueAtColumn(colNum));
    return value == null ? 0 : value;
  }

  @Override
  public long getLong(String column) throws SQLException {
    return this.getLong(getColumnIndex(column));
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    return Optional.ofNullable(getValueAtColumn(columnIndex))
        .map(v -> BaseType.isNull(v) ? null : v)
        .map(Byte::parseByte)
        .orElse((byte) 0);
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    return BaseType.SHORT.transform(getValueAtColumn(columnIndex));
  }

  @Override
  public byte getByte(String column) throws SQLException {
    return this.getByte(getColumnIndex(column));
  }

  @Override
  public short getShort(String columnLabel) throws SQLException {
    return this.getShort(getColumnIndex(columnLabel));
  }

  @Override
  public byte[] getBytes(int colNum) throws SQLException {
    return Optional.ofNullable(getValueAtColumn(colNum))
        .map(v -> BaseType.isNull(v) ? null : v)
        .map(String::getBytes)
        .orElse(null);
  }

  @Override
  public byte[] getBytes(String column) throws SQLException {
    return this.getBytes(getColumnIndex(column));
  }

  @Override
  public void close() throws SQLException {
    if (!this.isClosed) {
      try {
        this.reader.close();
        this.isClosed = true;
      } catch (IOException e) {
        throw new SQLException("Could not close data stream when closing ResultSet", e);
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
    return BaseType.DECIMAL.transform(value);
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    return getBigDecimal(this.getColumnIndex(columnLabel));
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    BigDecimal bigDecimal = this.getBigDecimal(columnIndex);
    return bigDecimal == null ? null : bigDecimal.setScale(scale, RoundingMode.HALF_UP);
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
    return getBigDecimal(this.getColumnIndex(columnLabel), scale);
  }

  @Override
  public Array getArray(int columnIndex) throws SQLException {
    String value = getValueAtColumn(columnIndex);
    return BaseType.ARRAY.transform(value, this.resultSetMetaData.getColumn(columnIndex));
  }

  @Override
  public Array getArray(String column) throws SQLException {
    return this.getArray(this.getColumnIndex(column));
  }

  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {
    return getBoolean(this.getColumnIndex(columnLabel));
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    String value = this.getValueAtColumn(columnIndex);
    return !"0".equals(value);
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException {
    return getDate(this.getColumnIndex(columnLabel));
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException {
    String value = this.getValueAtColumn(columnIndex);
    return BaseType.DATE.transform(value);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    String value = this.getValueAtColumn(columnIndex);
    return BaseType.TIMESTAMP.transform(value);
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    return getTimestamp(this.getColumnIndex(columnLabel));
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar calendar) throws SQLException {
    return this.getTimestamp(columnIndex);
  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar calendar) throws SQLException {
    return getTimestamp(this.getColumnIndex(columnLabel), calendar);
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException {
    return getTime(this.getColumnIndex(columnLabel));
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {
    return BaseType.TIME.transform(this.getValueAtColumn(columnIndex));
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

    FireboltColumn columnInfo = this.columns.get(columnIndex - 1);
    FireboltDataType columnType = columnInfo.getDataType();
    Object object = columnType.getBaseType().transform(value, columnInfo);
    if (columnType == FireboltDataType.ARRAY && object != null) {
      return ((FireboltArray) object).getArray();
    } else {
      return object;
    }
  }

  @Override
  public Object getObject(String column) throws SQLException {
    return getObject(getColumnIndex(column));
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
    return wasNull;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    throw new UnsupportedOperationException(
        String.format(FEATURE_NOT_SUPPORTED_YET, new Throwable().getStackTrace()[0].getMethodName()));
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    throw new UnsupportedOperationException(
        String.format(FEATURE_NOT_SUPPORTED_YET, new Throwable().getStackTrace()[0].getMethodName()));
  }

  @Override
  public boolean first() throws SQLException {
    throw new SQLException("Cannot call first() for ResultSet of type TYPE_FORWARD_ONLY");
  }

  @Override
  public boolean last() throws SQLException {
    throw new SQLException("Cannot call last() for ResultSet of type TYPE_FORWARD_ONLY");
  }

  private String[] toStringArray(String stringToSplit) {
    if (currentRow != lastSplittedRow) {
      if (StringUtils.isNotEmpty(stringToSplit)) {
        arr = StringUtils.splitPreserveAllTokens(stringToSplit, '\t');
      } else if (StringUtils.equals(stringToSplit, "")) {
        arr = new String[] {""};
      } else {
        arr = new String[0];
      }
      lastSplittedRow = currentRow;
    }
    return arr;
  }

  private List<FireboltColumn> getColumns(String[] columnNames, String columnTypes) {
    log.info("Column names {}", (Object) columnNames);
    log.info("Column types {}", columnTypes);
    String[] types = toStringArray(columnTypes);

    log.info(
        "column types length: {} columnName length: {}", columnTypes.length(), columnNames.length);
    return IntStream.range(0, types.length)
        .mapToObj(i -> FireboltColumn.of(types[i], StringEscapeUtils.unescapeJava(columnNames[i])))
        .collect(Collectors.toList());
  }

  private String getValueAtColumn(int columnIndex) throws SQLException {
    checkStreamNotClosed();
    return toStringArray(currentLine)[getColumnIndex(columnIndex)];
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
          String.format(
              "There is no column with number %d. Total of of columns available: %d ",
              columnNumber, columns.size()));
    }
  }

  private Integer getColumnIndex(String columnName) throws SQLException {
    Integer index = columnNameToColumnNumber.get(columnName);
    if (index == null) {
      throw new SQLException(String.format("There is no column with name %s ", columnName));
    }
    return index;
  }
}
