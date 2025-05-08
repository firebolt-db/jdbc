package com.firebolt.jdbc.statement.preparedstatement;

import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.exception.ExceptionType;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.service.FireboltStatementService;
import com.firebolt.jdbc.statement.StatementInfoWrapper;
import com.firebolt.jdbc.type.JavaTypeToFireboltSQLString;
import lombok.CustomLog;
import lombok.NonNull;
import org.json.JSONArray;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import static com.firebolt.jdbc.statement.StatementUtil.prepareFbNumericStatement;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.NUMERIC;

@CustomLog
public class FireboltBackendPreparedStatement extends FireboltPreparedStatement {

    public FireboltBackendPreparedStatement(FireboltStatementService statementService, FireboltConnection connection, String sql) {
        super(statementService, connection, sql);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        validateStatementIsNotClosed();
        providedParameters.put(parameterIndex, null);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        validateStatementIsNotClosed();
        providedParameters.put(parameterIndex, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        validateStatementIsNotClosed();
        providedParameters.put(parameterIndex, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        validateStatementIsNotClosed();
        providedParameters.put(parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        validateStatementIsNotClosed();
        providedParameters.put(parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        validateStatementIsNotClosed();
        providedParameters.put(parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        validateStatementIsNotClosed();
        providedParameters.put(parameterIndex, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        validateStatementIsNotClosed();
        providedParameters.put(parameterIndex, x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        validateStatementIsNotClosed();
        providedParameters.put(parameterIndex, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        validateStatementIsNotClosed();
        providedParameters.put(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        validateStatementIsNotClosed();
        providedParameters.put(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        validateStatementIsNotClosed();
        providedParameters.put(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        validateStatementIsNotClosed();
        try {
            JavaTypeToFireboltSQLString.validateObjectIsOfSupportedType(x);
            //this is used as a supported target type verification. null is not supported
            if (x != null) {
                JavaTypeToFireboltSQLString.getType(targetSqlType, x);
            }
            providedParameters.put(parameterIndex, x);
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
        JavaTypeToFireboltSQLString.validateObjectIsOfSupportedType(x);
        providedParameters.put(parameterIndex, x);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        validateStatementIsNotClosed();
        providedParameters.put(parameterIndex, null);
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        validateStatementIsNotClosed();
        providedParameters.put(parameterIndex, new JSONArray(x.getArray()).toString());
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        validateStatementIsNotClosed();
        try {
            boolean isNumber = (DECIMAL == targetSqlType || NUMERIC == targetSqlType) && x instanceof Number;
            //convert object to a string if it is not a number
            String str = isNumber ? formatDecimalNumber(x, scaleOrLength) : JavaTypeToFireboltSQLString.transformAny(x, targetSqlType);
            providedParameters.put(parameterIndex, Double.valueOf(str));
        } catch (FireboltException fbe) {
            if (ExceptionType.TYPE_NOT_SUPPORTED.equals(fbe.getType())) {
                throw new SQLFeatureNotSupportedException(fbe.getMessage(), fbe);
            }
        }
    }

    @Override
    protected List<StatementInfoWrapper> prepareSQL(@NonNull Map<Integer, Object> params) {
        return prepareFbNumericStatement(params, rawStatement);
    }

    @Override
    protected <T extends java.util.Date> void setDateTime(int parameterIndex, T datetime, Calendar calendar, JavaTypeToFireboltSQLString type) throws SQLException {
        validateStatementIsNotClosed();
        if (datetime == null || calendar == null) {
            providedParameters.put(parameterIndex, datetime);
        } else {
            String timeZoneId = calendar.getTimeZone().getID();
            providedParameters.put(parameterIndex, type.transformDateTimeForServerSide(datetime, timeZoneId));
        }
    }
}
