package io.firebolt.jdbc.preparedstatement;

import io.firebolt.QueryUtil;
import io.firebolt.jdbc.connection.FireboltConnectionTokens;
import io.firebolt.jdbc.connection.settings.FireboltProperties;
import io.firebolt.jdbc.exception.FireboltException;
import io.firebolt.jdbc.resultset.type.JavaTypeToStringConverter;
import io.firebolt.jdbc.service.FireboltQueryService;
import lombok.Builder;
import org.apache.commons.lang3.RegExUtils;

import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class FireboltPreparedStatement extends AbstractPreparedStatement {

  private static final String REGEX_OF_UNDEFINED_PARAM = " \\?|,\\?|\\(\\?";
  private final String sql;
  List<Map<Integer, String>> rows;
  private Map<Integer, String> currentParams;

  @Builder(
      builderMethodName =
          "statementBuilder") // As the parent is also using @Builder, a method name is mandatory
  public FireboltPreparedStatement(
      FireboltQueryService fireboltQueryService,
      FireboltProperties sessionProperties,
      FireboltConnectionTokens connectionTokens,
      String sql, Connection connection) {
    super(fireboltQueryService, sessionProperties, connectionTokens, connection);
    this.sql = sql;
    this.currentParams = new HashMap<>();
    this.rows = new ArrayList<>();
  }

  @Override
  public ResultSet executeQuery() throws SQLException {
    return super.executeQuery(prepareSQL(this.currentParams));
  }

  private String prepareSQL(Map<Integer, String> params) {
    String tmpSql = this.sql;
    for (int i = 1; i <= params.keySet().size(); i++) {
      String value = params.get(i);
      if (value == null) {
        throw new IllegalArgumentException("No value for ? at position: " + i);
      }
      tmpSql =
          RegExUtils.replaceFirst(tmpSql, "\\?", value.startsWith("\\") ? "\\" + value : value);
    }
    if (Pattern.compile(REGEX_OF_UNDEFINED_PARAM).matcher(tmpSql).find()) {
      throw new IllegalArgumentException("Some parameters are still undefined :" + tmpSql);
    } else {
      return tmpSql;
    }
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
    this.setObject(parameterIndex, x);
  }

  @Override
  public void setObject(int parameterIndex, Object x) throws SQLException {
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
}
