package io.firebolt.jdbc.resultset;

import io.firebolt.jdbc.exception.FireboltException;
import io.firebolt.jdbc.resultset.type.BaseType;
import io.firebolt.jdbc.resultset.type.FireboltDataType;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
@EqualsAndHashCode(callSuper = true)
public class FireboltResultSet extends AbstractResultSet {
  private final BufferedReader reader;
  private final Map<String, Integer> columnNameToColumnNumber;
  private final List<FireboltColumn> columns;
  private final FireboltResultSetMetaData resultSetMetaData;
  boolean wasNull = false;
  private String currentLine;
  private int pos = 0;
  private int splitPos = -1;
  private String[] arr;
  private boolean isClosed = false;

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
    this(is, null, null, null);
  }

  public FireboltResultSet(InputStream is, String tableName, String dbName, Integer bufferSize)
      throws SQLException {
    log.debug("Creating resultSet...");
    this.reader =
        bufferSize != null
            ? new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8), bufferSize)
            : new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
    try {
      this.next();
      String[] fields = toStringArray(currentLine);
      this.columnNameToColumnNumber = getColumnNamesToIndexes(fields);
      this.next();
      this.columns = getColumns(fields, currentLine);
      resultSetMetaData = FireboltResultSetMetaData.builder().columns(columns).tableName(tableName).dbName(dbName).build();
      log.debug("ResultSetMetaData created");
    } catch (Exception e) {
      log.error("Could not create ResultSet: "+ ExceptionUtils.getStackTrace(e), e);
      throw new FireboltException("Cannot read response from DB: error while creating ResultSet", e);
    }
    log.debug("ResultSet created");
  }

  public static FireboltResultSet empty() {
    return new FireboltResultSet();
  }

  @Override
  public boolean next() throws SQLException {
    checkStreamNotClosed();

    try {
      currentLine = reader.readLine();
      pos++;
    } catch (IOException e) {
      throw new SQLException("Error reading result from stream", e);
    }

    return currentLine != null;
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    return this.getValueAtColumn(columnIndex);
  }

  @Override
  public String getString(String column) throws SQLException {
    return getString(getColumnIndex(column));
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    return Integer.parseInt(getValueAtColumn(columnIndex));
  }

  @Override
  public int getInt(String columnName) throws SQLException {
    return Integer.parseInt(getValueAtColumn(getColumnIndex(columnName)));
  }

  @Override
  public long getLong(int colNum) throws SQLException {
    return Long.parseLong(getValueAtColumn(colNum));
  }

  @Override
  public long getLong(String column) throws SQLException {
    return getLong(getColumnIndex(column));
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    return Optional.ofNullable(getValueAtColumn(columnIndex)).map(Byte::parseByte).orElse((byte) 0);
  }

  @Override
  public byte getByte(String column) throws SQLException {
    return this.getByte(getColumnIndex(column));
  }

  @Override
  public byte[] getBytes(int colNum) throws SQLException {
    return Optional.ofNullable(getValueAtColumn(colNum)).map(String::getBytes).orElse(null);
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
    return BaseType.ARRAY.transform(
        value, this.resultSetMetaData.getColumn(columnIndex).getArrayBaseDataType());
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
    Timestamp ts = getTimestamp(columnIndex);
    if (ts == null) {
      return null;
    }

    return new Time(ts.getTime());
  }

  @Override
  public int getRow() {
    return pos;
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
    if (null == value) {
      return null;
    }

    FireboltColumn columnInfo = this.columns.get(columnIndex - 1);
    FireboltDataType columnType = columnInfo.getDataType();

    return Optional.of(columnType)
        .map(type -> type.getBaseType().transform(value, columnInfo.getArrayBaseDataType()))
        .orElse(null);
  }

  @Override
  public Object getObject(String column) throws SQLException {
    return getObject(getColumnIndex(column));
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    checkStreamNotClosed();
    return pos < 3 || !hasNext();
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
    return pos == 3;
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
    throw new UnsupportedOperationException();
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    throw new UnsupportedOperationException();
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
    if (pos != splitPos) {
      arr = StringUtils.splitPreserveAllTokens(stringToSplit, '\t');
      splitPos = pos;
    }
    return arr;
  }

  private List<FireboltColumn> getColumns(String[] columnNames, String columnTypes) {
    String[] types = toStringArray(columnTypes);
    return IntStream.range(0, types.length)
        .mapToObj(i -> FireboltColumn.of(types[i], StringEscapeUtils.unescapeJava(columnNames[i])))
        .collect(Collectors.toList());
  }

  private String getValueAtColumn(int columnIndex) throws SQLException {
    checkStreamNotClosed();
    String value = toStringArray(currentLine)[getColumnIndex(columnIndex)];
    value = BaseType.STRING.transform(value);
    wasNull = null == value;
    return value;
  }

  private int getColumnIndex(int colNum) throws SQLException {
    validateColumnNumber(colNum);
    return colNum - 1;
  }

  private Map<String, Integer> getColumnNamesToIndexes(String[] fields) {
    Map<String, Integer> columnNameToFieldIndex = new HashMap<>();
    for (int i = 0; i < fields.length; i++) {
      columnNameToFieldIndex.put(fields[i], i + 1);
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
