package io.firebolt.jdbc.preparedstatement;

import io.firebolt.QueryUtil;
import io.firebolt.jdbc.connection.FireboltConnectionImpl;
import io.firebolt.jdbc.connection.settings.FireboltProperties;
import io.firebolt.jdbc.exception.FireboltException;
import io.firebolt.jdbc.resultset.type.JavaTypeToStringConverter;
import io.firebolt.jdbc.service.FireboltQueryService;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class FireboltPreparedStatement extends AbstractPreparedStatement {

  private final String sql;
  private final String cleanSql;
  private final List<Map<Integer, String>> rows;
  private Map<Integer, String> currentParams;

  private final int totalParams;

  @Builder(
      builderMethodName =
          "statementBuilder") // As the parent is also using @Builder, a method name is mandatory
  public FireboltPreparedStatement(
      FireboltQueryService fireboltQueryService,
      FireboltProperties sessionProperties,
      String accessToken,
      String sql,
      FireboltConnectionImpl connection) {
    super(fireboltQueryService, sessionProperties, accessToken, connection);
    log.debug("Populating PreparedStatement object for SQL: {}", sql);
    this.sql = sql;
    Pair<String, Integer> cleanQueryWithParamCount =
        QueryUtil.cleanQueryAndCountKeyWordOccurrences(sql, "?");
    this.cleanSql = cleanQueryWithParamCount.getLeft();
    this.totalParams = cleanQueryWithParamCount.getRight();
    this.currentParams = new HashMap<>();
    this.rows = new ArrayList<>();
    log.debug("PreparedStatement initialized for SQL: {}", this.cleanSql);
  }

  @Override
  public ResultSet executeQuery() throws SQLException {
    return super.executeQuery(prepareSQL(this.currentParams));
  }

  private String prepareSQL(Map<Integer, String> params) {
    log.debug("Preparing SQL for query: {}", cleanSql);
    String tmpSql = cleanSql;

    if (!params.keySet().isEmpty()) {
      tmpSql = replaceQuestionMarksWithParams(params, tmpSql);
    }
    if (params.size() < this.totalParams) {
      throw new IllegalArgumentException("Some parameters are still undefined :" + tmpSql);
    } else {
      log.debug("Prepared SQL for query: {}, result: {}", sql, tmpSql);
      return tmpSql;
    }
  }

  private String replaceQuestionMarksWithParams(Map<Integer, String> params, String tmpSql) {
    int currentPos = 0;
    char currentChar = tmpSql.charAt(currentPos);
    boolean isBetweenQuotes = currentChar == '\'';
    boolean questionMarkFound = false;
    for (int i = 1; i <= params.keySet().size(); i++) {
      String value = params.get(i);
      if (value == null) {
        throw new IllegalArgumentException("No value for ? at position: " + i);
      }
      while (!questionMarkFound && currentPos < tmpSql.length() - 1) {
        currentPos++;
        currentChar = tmpSql.charAt(currentPos);
        if (currentChar == '\'') {
          isBetweenQuotes = !isBetweenQuotes;
        }
        questionMarkFound = '?' == currentChar && !isBetweenQuotes;
      }
      tmpSql = tmpSql.substring(0, currentPos) + value + tmpSql.substring(currentPos + 1);
      currentPos = currentPos + value.length();
      questionMarkFound = false;
    }
    return tmpSql;
  }

  @Override
  public int executeUpdate() throws SQLException {
    return super.executeUpdate(prepareSQL(this.currentParams));
  }

  @Override
  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    this.currentParams.put(parameterIndex, "\\N");
  }

  @Override
  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    this.currentParams.put(
        parameterIndex,
        JavaTypeToStringConverter.BOOLEAN.getTransformToJavaTypeFunction().apply(x));
  }

  @Override
  public void setByte(int parameterIndex, byte x) throws SQLException {
    throw new SQLFeatureNotSupportedException("The format Byte is currently not supported");
  }

  @Override
  public void setShort(int parameterIndex, short x) throws SQLException {
    this.currentParams.put(
        parameterIndex, JavaTypeToStringConverter.SHORT.getTransformToJavaTypeFunction().apply(x));
  }

  @Override
  public void setInt(int parameterIndex, int x) throws SQLException {
    this.currentParams.put(
        parameterIndex,
        JavaTypeToStringConverter.INTEGER.getTransformToJavaTypeFunction().apply(x));
  }

  @Override
  public void setLong(int parameterIndex, long x) throws SQLException {
    this.currentParams.put(
        parameterIndex, JavaTypeToStringConverter.LONG.getTransformToJavaTypeFunction().apply(x));
  }

  @Override
  public void setFloat(int parameterIndex, float x) throws SQLException {
    this.currentParams.put(
        parameterIndex, JavaTypeToStringConverter.FLOAT.getTransformToJavaTypeFunction().apply(x));
  }

  @Override
  public void setDouble(int parameterIndex, double x) throws SQLException {
    this.currentParams.put(
        parameterIndex, JavaTypeToStringConverter.DOUBLE.getTransformToJavaTypeFunction().apply(x));
  }

  @Override
  public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    this.currentParams.put(
        parameterIndex,
        JavaTypeToStringConverter.BIG_DECIMAL.getTransformToJavaTypeFunction().apply(x));
  }

  @Override
  public void setString(int parameterIndex, String x) throws SQLException {
    this.currentParams.put(parameterIndex, x);
  }

  @Override
  public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    throw new SQLFeatureNotSupportedException("The format Byte is currently not supported");
  }

  @Override
  public void setDate(int parameterIndex, Date x) throws SQLException {
    this.currentParams.put(
        parameterIndex, JavaTypeToStringConverter.DATE.getTransformToJavaTypeFunction().apply(x));
  }

  @Override
  public void setTime(int parameterIndex, Time x) throws SQLException {
    throw new SQLFeatureNotSupportedException("The format Time is currently not supported");
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    this.currentParams.put(
        parameterIndex,
        JavaTypeToStringConverter.TIMESTAMP.getTransformToJavaTypeFunction().apply(x));
  }

  @Override
  public void clearParameters() throws SQLException {
    this.currentParams.clear();
    this.rows.clear();
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
    this.validateParamIndex(parameterIndex);
    this.setObject(parameterIndex, x);
  }

  @Override
  public void setObject(int parameterIndex, Object x) throws SQLException {
    this.validateParamIndex(parameterIndex);
    currentParams.put(parameterIndex, JavaTypeToStringConverter.toString(x));
  }

  @Override
  public boolean execute() throws SQLException {
    return super.execute(prepareSQL(currentParams));
  }

  @Override
  public void addBatch() throws SQLException {
    if (QueryUtil.isSelect(this.sql)) {
      throw new FireboltException("Cannot call addBatch() for SELECT queries");
    } else {
      rows.add(this.currentParams);
      this.currentParams = new HashMap<>();
    }
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    ResultSet resultSet = this.getResultSet();
    if (resultSet != null) {
      return resultSet.getMetaData();
    } else {
      return null;
    }
  }

  @Override
  public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
    this.validateParamIndex(parameterIndex);
    this.currentParams.put(parameterIndex, "\\N");
  }

  @Override
  public void setURL(int parameterIndex, URL x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setNString(int parameterIndex, String value) throws SQLException {
    this.setString(parameterIndex, value);
  }

  @Override
  public int[] executeBatch() throws SQLException {
    log.debug("Executing batch for query: {}", sql);
    List<String> inserts = new ArrayList<>();
    int[] result = new int[this.rows.size()];
    for (Map<Integer, String> row : rows) {
      inserts.add(this.prepareSQL(row));
    }
    for (int i = 0; i < inserts.size(); i++) {
      result[i] = 1;
      this.execute(inserts.get(i));
    }
    return result;
  }

  private void validateParamIndex(int paramIndex) throws FireboltException {
    if (this.totalParams < paramIndex) {
      throw new FireboltException(
          String.format(
              "Cannot not set parameter as there is no parameter at index: %d for query: %s",
              paramIndex, this.sql));
    }
  }
}
