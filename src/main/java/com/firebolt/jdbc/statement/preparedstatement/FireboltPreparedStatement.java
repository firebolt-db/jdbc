package com.firebolt.jdbc.statement.preparedstatement;

import com.firebolt.jdbc.annotation.ExcludeFromJacocoGeneratedReport;
import com.firebolt.jdbc.annotation.NotImplemented;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.exception.FireboltSQLFeatureNotSupportedException;
import com.firebolt.jdbc.exception.FireboltUnsupportedOperationException;
import com.firebolt.jdbc.service.FireboltStatementService;
import com.firebolt.jdbc.statement.FireboltStatement;
import com.firebolt.jdbc.statement.StatementInfoWrapper;
import com.firebolt.jdbc.statement.StatementUtil;
import com.firebolt.jdbc.statement.rawstatement.RawStatementWrapper;
import com.firebolt.jdbc.type.JavaTypeToFireboltSQLString;
import lombok.Builder;
import lombok.CustomLog;
import lombok.NonNull;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Date;
import java.sql.*;
import java.util.*;

import static com.firebolt.jdbc.statement.StatementUtil.replaceParameterMarksWithValues;

@CustomLog
public class FireboltPreparedStatement extends FireboltStatement implements PreparedStatement {

	private final RawStatementWrapper rawStatement;
	private final List<Map<Integer, String>> rows;
	private Map<Integer, String> providedParameters;

	@Builder(builderMethodName = "statementBuilder") // As the parent is also using @Builder, a method name is mandatory
	public FireboltPreparedStatement(FireboltStatementService statementService, FireboltProperties sessionProperties,
			String sql, FireboltConnection connection) {
		super(statementService, sessionProperties, connection);
		log.debug("Populating PreparedStatement object for SQL: {}", sql);
		this.providedParameters = new HashMap<>();
		this.rawStatement = StatementUtil.parseToRawStatementWrapper(sql);
		this.rows = new ArrayList<>();
	}

	@Override
	public ResultSet executeQuery() throws SQLException {
		List<StatementInfoWrapper> rawStatementWrapper = prepareSQL(providedParameters);
		return super.executeQuery(rawStatementWrapper);
	}

	private List<StatementInfoWrapper> prepareSQL(@NonNull Map<Integer, String> params) {
		return replaceParameterMarksWithValues(params, this.rawStatement);
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
		throw new FireboltSQLFeatureNotSupportedException();
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
		rows.add(this.providedParameters);
		this.providedParameters = new HashMap<>();
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
		throw new FireboltSQLFeatureNotSupportedException();
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
		log.debug("Executing batch for statement: {}", rawStatement);
		List<StatementInfoWrapper> inserts = new ArrayList<>();
		int[] result = new int[this.rows.size()];
		for (Map<Integer, String> row : rows) {
			inserts.addAll(this.prepareSQL(row));
		}
		this.execute(inserts);
		for (int i = 0; i < inserts.size(); i++) {
			result[i] = SUCCESS_NO_INFO;
		}
		return result;
	}

	@Override
	public int executeUpdate(String sql) throws SQLException {
		throw new FireboltException("Cannot call method executeUpdate(String sql) on a PreparedStatement");
	}

	private void validateParamIndex(int paramIndex) throws FireboltException {
		if (this.rawStatement.getTotalParams() < paramIndex) {
			throw new FireboltException(String.format(
					"Cannot set parameter as there is no parameter at index: %d for statement: %s", paramIndex, rawStatement));
		}
	}

	@Override
	public boolean execute(String sql) throws SQLException {
		throw new FireboltException("Cannot call execute(String) on a PreparedStatement");
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void setRef(int parameterIndex, Ref x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void setBlob(int parameterIndex, Blob x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void setClob(int parameterIndex, Clob x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public ParameterMetaData getParameterMetaData() throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void setRowId(int parameterIndex, RowId x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void setClob(int parameterIndex, Reader reader) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}
}
