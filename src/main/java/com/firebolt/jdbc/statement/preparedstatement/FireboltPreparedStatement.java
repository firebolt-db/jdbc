package com.firebolt.jdbc.statement.preparedstatement;

import com.firebolt.jdbc.annotation.NotImplemented;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.ExceptionType;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.exception.FireboltSQLFeatureNotSupportedException;
import com.firebolt.jdbc.exception.FireboltUnsupportedOperationException;
import com.firebolt.jdbc.service.FireboltStatementService;
import com.firebolt.jdbc.statement.FireboltStatement;
import com.firebolt.jdbc.statement.StatementInfoWrapper;
import com.firebolt.jdbc.statement.StatementUtil;
import com.firebolt.jdbc.statement.rawstatement.RawStatementWrapper;
import com.firebolt.jdbc.type.JavaTypeToFireboltSQLString;
import com.firebolt.jdbc.util.InputStreamUtil;
import lombok.CustomLog;
import lombok.NonNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.firebolt.jdbc.statement.StatementUtil.replaceParameterMarksWithValues;
import static com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory.createValidator;
import static java.lang.String.format;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.VARBINARY;

@CustomLog
public class FireboltPreparedStatement extends FireboltStatement implements PreparedStatement {

	private final RawStatementWrapper rawStatement;
	private final List<Map<Integer, String>> rows;
	private Map<Integer, String> providedParameters;

	public FireboltPreparedStatement(FireboltStatementService statementService, FireboltConnection connection, String sql) {
		this(statementService, connection.getSessionProperties(), connection, sql);
	}

	public FireboltPreparedStatement(FireboltStatementService statementService, FireboltProperties sessionProperties,
									 FireboltConnection connection, String sql) {
		super(statementService, sessionProperties, connection);
		log.debug("Populating PreparedStatement object for SQL: {}", sql);
		this.providedParameters = new HashMap<>();
		this.rawStatement = StatementUtil.parseToRawStatementWrapper(sql);
		rawStatement.getSubStatements().forEach(statement -> createValidator(statement, connection).validate(statement));
		this.rows = new ArrayList<>();
	}

	@Override
	public ResultSet executeQuery() throws SQLException {
		List<StatementInfoWrapper> rawStatementWrapper = prepareSQL(providedParameters);
		return super.executeQuery(rawStatementWrapper);
	}

	private List<StatementInfoWrapper> prepareSQL(@NonNull Map<Integer, String> params) {
		return replaceParameterMarksWithValues(params, rawStatement);
	}

	@Override
	public int executeUpdate() throws SQLException {
		validateStatementIsNotClosed();
		return super.executeUpdate(prepareSQL(providedParameters));
	}

	@Override
	public void setNull(int parameterIndex, int sqlType) throws SQLException {
		validateStatementIsNotClosed();
		validateParamIndex(parameterIndex);
		providedParameters.put(parameterIndex, JavaTypeToFireboltSQLString.NULL_VALUE);
	}

	@Override
	public void setBoolean(int parameterIndex, boolean x) throws SQLException {
		validateStatementIsNotClosed();
		validateParamIndex(parameterIndex);
		providedParameters.put(parameterIndex, JavaTypeToFireboltSQLString.BOOLEAN.transform(x));
	}

	@Override
	public void setByte(int parameterIndex, byte x) throws SQLException {
		validateStatementIsNotClosed();
		validateParamIndex(parameterIndex);
		providedParameters.put(parameterIndex, JavaTypeToFireboltSQLString.BYTE.transform(x));
	}

	@Override
	public void setShort(int parameterIndex, short x) throws SQLException {
		validateStatementIsNotClosed();
		validateParamIndex(parameterIndex);
		providedParameters.put(parameterIndex, JavaTypeToFireboltSQLString.SHORT.transform(x));
	}

	@Override
	public void setInt(int parameterIndex, int x) throws SQLException {
		validateStatementIsNotClosed();
		validateParamIndex(parameterIndex);
		providedParameters.put(parameterIndex, JavaTypeToFireboltSQLString.INTEGER.transform(x));
	}

	@Override
	public void setLong(int parameterIndex, long x) throws SQLException {
		validateStatementIsNotClosed();
		validateParamIndex(parameterIndex);
		providedParameters.put(parameterIndex, JavaTypeToFireboltSQLString.LONG.transform(x));
	}

	@Override
	public void setFloat(int parameterIndex, float x) throws SQLException {
		validateStatementIsNotClosed();
		validateParamIndex(parameterIndex);
		providedParameters.put(parameterIndex, JavaTypeToFireboltSQLString.FLOAT.transform(x));
	}

	@Override
	public void setDouble(int parameterIndex, double x) throws SQLException {
		validateStatementIsNotClosed();
		validateParamIndex(parameterIndex);
		providedParameters.put(parameterIndex, JavaTypeToFireboltSQLString.DOUBLE.transform(x));
	}

	@Override
	public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
		validateStatementIsNotClosed();
		validateParamIndex(parameterIndex);
		providedParameters.put(parameterIndex, JavaTypeToFireboltSQLString.BIG_DECIMAL.transform(x));
	}

	@Override
	public void setString(int parameterIndex, String x) throws SQLException {
		validateStatementIsNotClosed();
		validateParamIndex(parameterIndex);
		providedParameters.put(parameterIndex, JavaTypeToFireboltSQLString.STRING.transform(x));
	}

	@Override
	public void setBytes(int parameterIndex, byte[] bytes) throws SQLException {
		if (bytes == null) {
			setNull(parameterIndex, VARBINARY);
		} else {
			setObject(parameterIndex, bytes, VARBINARY);
		}
	}

	@Override
	public void setDate(int parameterIndex, Date x) throws SQLException {
		validateStatementIsNotClosed();
		validateParamIndex(parameterIndex);
		providedParameters.put(parameterIndex, JavaTypeToFireboltSQLString.DATE.transform(x));
	}

	@Override
	@NotImplemented
	public void setTime(int parameterIndex, Time x) throws SQLException {
		throw new SQLFeatureNotSupportedException("The format Time is currently not supported");
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
		validateStatementIsNotClosed();
		validateParamIndex(parameterIndex);
		providedParameters.put(parameterIndex, JavaTypeToFireboltSQLString.TIMESTAMP.transform(x));
	}

	@Override
	public void clearParameters() throws SQLException {
		providedParameters.clear();
		rows.clear();
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
		validateStatementIsNotClosed();
		validateParamIndex(parameterIndex);
		try {
			providedParameters.put(parameterIndex, JavaTypeToFireboltSQLString.transformAny(x, targetSqlType));
		} catch (FireboltException fbe) {
			if (ExceptionType.TYPE_NOT_SUPPORTED.equals(fbe.getType())) {
				throw new SQLFeatureNotSupportedException(fbe.getMessage(), fbe);
			}
			throw fbe;
		}
	}

	@Override
	public void setObject(int parameterIndex, Object x) throws SQLException {
		validateStatementIsNotClosed();
		validateParamIndex(parameterIndex);
		providedParameters.put(parameterIndex, JavaTypeToFireboltSQLString.transformAny(x));
	}

	@Override
	public boolean execute() throws SQLException {
		validateStatementIsNotClosed();
		return super.execute(prepareSQL(providedParameters)).isPresent();
	}

	@Override
	public void addBatch() throws SQLException {
		rows.add(providedParameters);
		providedParameters = new HashMap<>();
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		ResultSet resultSet = getResultSet();
		return resultSet != null ? resultSet.getMetaData() : null;
	}

	@Override
	public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
		validateStatementIsNotClosed();
		validateParamIndex(parameterIndex);
		providedParameters.put(parameterIndex, JavaTypeToFireboltSQLString.NULL_VALUE);
	}

	@Override
	public void setURL(int parameterIndex, URL url) throws SQLException {
		setString(parameterIndex, url == null ? null : url.toString());
	}

	@Override
	public void setNString(int parameterIndex, String value) throws SQLException {
		validateStatementIsNotClosed();
		validateParamIndex(parameterIndex);
		setString(parameterIndex, value);
	}

	@Override
	public void setArray(int parameterIndex, Array x) throws SQLException {
		validateStatementIsNotClosed();
		validateParamIndex(parameterIndex);
		providedParameters.put(parameterIndex, JavaTypeToFireboltSQLString.ARRAY.transform(x));
	}

	@Override
	public int[] executeBatch() throws SQLException {
		validateStatementIsNotClosed();
		log.debug("Executing batch for statement: {}", rawStatement);
		List<StatementInfoWrapper> inserts = new ArrayList<>();
		int[] result = new int[rows.size()];
		for (Map<Integer, String> row : rows) {
			inserts.addAll(prepareSQL(row));
		}
		execute(inserts);
		for (int i = 0; i < inserts.size(); i++) {
			result[i] = SUCCESS_NO_INFO;
		}
		return result;
	}

	@Override
	@NotImplemented
	public int executeUpdate(String sql) throws SQLException {
		throw new FireboltException("Cannot call method executeUpdate(String sql) on a PreparedStatement");
	}

	private void validateParamIndex(int paramIndex) throws FireboltException {
		if (rawStatement.getTotalParams() < paramIndex) {
			throw new FireboltException(
					format("Cannot set parameter as there is no parameter at index: %d for statement: %s",
							paramIndex, rawStatement));
		}
	}

	@Override
	public boolean execute(String sql) throws SQLException {
		throw new FireboltException("Cannot call execute(String) on a PreparedStatement");
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
		try {
			setString(parameterIndex, reader == null ? null : InputStreamUtil.read(reader, length));
		} catch (IOException e) {
			throw new SQLException(e);
		}
	}

	@Override
	@NotImplemented
	public void setRef(int parameterIndex, Ref x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	public void setBlob(int parameterIndex, Blob blob) throws SQLException {
		setBytes(parameterIndex, blob == null ? null : blob.getBytes(1, (int)blob.length()));
	}

	@Override
	public void setClob(int parameterIndex, Clob clob) throws SQLException {
		setString(parameterIndex, clob == null ? null : clob.getSubString(1, (int)clob.length()));
	}

	@Override
	@NotImplemented
	public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	@Override
	@NotImplemented
	public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	@Override
	@NotImplemented
	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	@Override
	@NotImplemented
	public ParameterMetaData getParameterMetaData() throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	@Override
	@NotImplemented
	public void setRowId(int parameterIndex, RowId x) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
		setCharacterStream(parameterIndex, value, length);
	}

	@Override
	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		setClob(parameterIndex, value);
	}

	@Override
	public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
		setCharacterStream(parameterIndex, reader, length);
    }

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
		setBinaryStream(parameterIndex, inputStream, length);
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
		setClob(parameterIndex, reader, length);
	}

	@Override
	@NotImplemented
	public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
		validateStatementIsNotClosed();
		validateParamIndex(parameterIndex);
		try {
			// scaleOfLength should affect only DECIMAL and NUMERIC types
			boolean isNumber = (DECIMAL == targetSqlType || NUMERIC == targetSqlType) && x instanceof Number;
			String str = isNumber ? formatDecimalNumber(x, scaleOrLength) : JavaTypeToFireboltSQLString.transformAny(x, targetSqlType);
			providedParameters.put(parameterIndex, str);
		} catch (FireboltException fbe) {
			if (ExceptionType.TYPE_NOT_SUPPORTED.equals(fbe.getType())) {
				throw new SQLFeatureNotSupportedException(fbe.getMessage(), fbe);
			}
		}
	}

	private String formatDecimalNumber(Object x, int scaleOrLength) {
		String format = format("%%.%df", scaleOrLength);
		return format(format, ((Number)x).doubleValue());
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
		setBinaryStream(parameterIndex, x, length);
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream inputStream, long length) throws SQLException {
		setBinaryStream(parameterIndex, inputStream, (int)length);
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
		setCharacterStream(parameterIndex, reader, (int)length);
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
		setBinaryStream(parameterIndex, x);
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
		try {
			setBytes(parameterIndex, x == null ? null : x.readAllBytes());
		} catch (IOException e) {
			throw new SQLException(e);
		}
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
		setCharacterStream(parameterIndex, reader, Integer.MAX_VALUE);
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
		setCharacterStream(parameterIndex, value);
	}

	@Override
	public void setClob(int parameterIndex, Reader reader) throws SQLException {
		setClob(parameterIndex, reader, Integer.MAX_VALUE);
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
		setBinaryStream(parameterIndex, inputStream);
    }

	@Override
	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
		setClob(parameterIndex, reader);
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
		setBinaryStream(parameterIndex, x, length);
	}

	@Override
	public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
		setBinaryStream(parameterIndex, x, length);
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream inputStream, int length) throws SQLException {
		try {
			setBytes(parameterIndex, inputStream == null ? null : inputStream.readNBytes(length));
		} catch (IOException e) {
			throw new SQLException(e);
		}
	}
}
