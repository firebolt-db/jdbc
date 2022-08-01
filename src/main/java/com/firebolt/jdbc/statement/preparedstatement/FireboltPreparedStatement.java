package com.firebolt.jdbc.statement.preparedstatement;

import static com.firebolt.jdbc.statement.StatementUtil.replaceParameterMarksWithValues;

import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.service.FireboltStatementService;
import com.firebolt.jdbc.statement.StatementUtil;
import com.firebolt.jdbc.type.JavaTypeToFireboltSQLString;

import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FireboltPreparedStatement extends AbstractPreparedStatement {

	private final String sql;
	private final List<Map<Integer, String>> rows;
	private final Map<Integer, Integer> parameterMarkerPositions;
	private Map<Integer, String> providedParameters;

	@Builder(builderMethodName = "statementBuilder") // As the parent is also using @Builder, a method name is mandatory
	public FireboltPreparedStatement(FireboltStatementService statementService, FireboltProperties sessionProperties,
			String sql, FireboltConnection connection) {
		super(statementService, sessionProperties, connection);
		log.debug("Populating PreparedStatement object for SQL: {}", sql);
		this.sql = sql;
		this.providedParameters = new HashMap<>();
		this.parameterMarkerPositions = StatementUtil.getQueryParamsPositions(sql);
		this.rows = new ArrayList<>();
	}

	@Override
	public ResultSet executeQuery() throws SQLException {
		return super.executeQuery(prepareSQL(this.providedParameters));
	}

	private String prepareSQL(@NonNull Map<Integer, String> params) throws SQLException {
		log.debug("Preparing SQL for statement: {}", this.sql);
		String result = this.sql;
		if (!this.parameterMarkerPositions.isEmpty()) {
			result = replaceParameterMarksWithValues(params, parameterMarkerPositions, result);
		}
		log.debug("Prepared SQL for query: {}, result: {}", sql, result);
		return result;
	}

	@Override
	public int executeUpdate() throws SQLException {
		this.validateStatementIsNotClosed();
		return super.executeUpdate(prepareSQL(this.providedParameters));
	}

	@Override
	public void setNull(int parameterIndex, int sqlType) throws SQLException {
		this.validateStatementIsNotClosed();
		this.validateParamIndex(parameterIndex);
		this.providedParameters.put(parameterIndex, "\\N");
	}

	@Override
	public void setBoolean(int parameterIndex, boolean x) throws SQLException {
		this.validateStatementIsNotClosed();
		this.validateParamIndex(parameterIndex);
		this.providedParameters.put(parameterIndex, JavaTypeToFireboltSQLString.BOOLEAN.transform(x));
	}

	@Override
	public void setByte(int parameterIndex, byte x) throws SQLException {
		throw new SQLFeatureNotSupportedException("The format Byte is currently not supported");
	}

	@Override
	public void setShort(int parameterIndex, short x) throws SQLException {
		this.validateStatementIsNotClosed();
		this.validateParamIndex(parameterIndex);
		this.providedParameters.put(parameterIndex, JavaTypeToFireboltSQLString.SHORT.transform(x));
	}

	@Override
	public void setInt(int parameterIndex, int x) throws SQLException {
		this.validateStatementIsNotClosed();
		this.validateParamIndex(parameterIndex);
		this.providedParameters.put(parameterIndex, JavaTypeToFireboltSQLString.INTEGER.transform(x));
	}

	@Override
	public void setLong(int parameterIndex, long x) throws SQLException {
		this.validateStatementIsNotClosed();
		this.validateParamIndex(parameterIndex);
		this.providedParameters.put(parameterIndex, JavaTypeToFireboltSQLString.LONG.transform(x));
	}

	@Override
	public void setFloat(int parameterIndex, float x) throws SQLException {
		this.validateStatementIsNotClosed();
		this.validateParamIndex(parameterIndex);
		this.providedParameters.put(parameterIndex, JavaTypeToFireboltSQLString.FLOAT.transform(x));
	}

	@Override
	public void setDouble(int parameterIndex, double x) throws SQLException {
		this.validateStatementIsNotClosed();
		this.validateParamIndex(parameterIndex);
		this.providedParameters.put(parameterIndex, JavaTypeToFireboltSQLString.DOUBLE.transform(x));
	}

	@Override
	public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
		this.validateStatementIsNotClosed();
		this.validateParamIndex(parameterIndex);
		this.providedParameters.put(parameterIndex, JavaTypeToFireboltSQLString.BIG_DECIMAL.transform(x));
	}

	@Override
	public void setString(int parameterIndex, String x) throws SQLException {
		this.validateStatementIsNotClosed();
		this.validateParamIndex(parameterIndex);
		this.providedParameters.put(parameterIndex, JavaTypeToFireboltSQLString.STRING.transform(x));
	}

	@Override
	public void setBytes(int parameterIndex, byte[] x) throws SQLException {
		throw new SQLFeatureNotSupportedException("The format Byte is currently not supported");
	}

	@Override
	public void setDate(int parameterIndex, Date x) throws SQLException {
		this.validateStatementIsNotClosed();
		this.validateParamIndex(parameterIndex);
		this.providedParameters.put(parameterIndex, JavaTypeToFireboltSQLString.DATE.transform(x));
	}

	@Override
	public void setTime(int parameterIndex, Time x) throws SQLException {
		throw new SQLFeatureNotSupportedException("The format Time is currently not supported");
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
		this.validateStatementIsNotClosed();
		this.validateParamIndex(parameterIndex);
		this.providedParameters.put(parameterIndex, JavaTypeToFireboltSQLString.TIMESTAMP.transform(x));
	}

	@Override
	public void clearParameters() throws SQLException {
		this.providedParameters.clear();
		this.rows.clear();
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
		this.validateStatementIsNotClosed();
		this.validateParamIndex(parameterIndex);
		this.setObject(parameterIndex, x);
	}

	@Override
	public void setObject(int parameterIndex, Object x) throws SQLException {
		this.validateStatementIsNotClosed();
		this.validateParamIndex(parameterIndex);
		providedParameters.put(parameterIndex, JavaTypeToFireboltSQLString.transformAny(x));
	}

	@Override
	public boolean execute() throws SQLException {
		this.validateStatementIsNotClosed();
		return super.execute(prepareSQL(providedParameters));
	}

	@Override
	public void addBatch() throws SQLException {
		if (StatementUtil.isQuery(this.sql)) {
			throw new FireboltException("Cannot call addBatch() for SELECT queries");
		} else {
			rows.add(this.providedParameters);
			this.providedParameters = new HashMap<>();
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
		this.validateStatementIsNotClosed();
		this.validateParamIndex(parameterIndex);
		this.providedParameters.put(parameterIndex, "\\N");
	}

	@Override
	public void setURL(int parameterIndex, URL x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setNString(int parameterIndex, String value) throws SQLException {
		this.validateStatementIsNotClosed();
		validateParamIndex(parameterIndex);
		this.setString(parameterIndex, value);
	}

	@Override
	public void setArray(int parameterIndex, Array x) throws SQLException {
		this.validateStatementIsNotClosed();
		validateParamIndex(parameterIndex);
		setString(parameterIndex, x.toString());
	}

	@Override
	public int[] executeBatch() throws SQLException {
		this.validateStatementIsNotClosed();
		log.debug("Executing batch for query: {}", sql);
		List<String> inserts = new ArrayList<>();
		int[] result = new int[this.rows.size()];
		for (Map<Integer, String> row : rows) {
			inserts.add(this.prepareSQL(row));
		}
		for (int i = 0; i < inserts.size(); i++) {
			this.execute(inserts.get(i));
			result[i] = SUCCESS_NO_INFO;
		}
		return result;
	}

	private void validateParamIndex(int paramIndex) throws FireboltException {
		if (!this.parameterMarkerPositions.containsKey(paramIndex)) {
			throw new FireboltException(
					String.format("Cannot not set parameter as there is no parameter at index: %d for query: %s",
							paramIndex, this.sql));
		}
	}
}
