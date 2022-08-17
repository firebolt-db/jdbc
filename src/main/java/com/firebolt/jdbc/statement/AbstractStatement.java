package com.firebolt.jdbc.statement;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import com.firebolt.jdbc.exception.FireboltUnsupportedOperationException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractStatement implements Statement {

	@Override
	public int executeUpdate(String sql) throws SQLException {
		return 0;
	}

	@Override
	public void close() throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	@Override
	public int getMaxFieldSize() throws SQLException {
		return 0;
	}

	@Override
	public void setMaxFieldSize(int max) throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	@Override
	public int getMaxRows() throws SQLException {
		return 0;
	}

	@Override
	public void setMaxRows(int max) throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	@Override
	public void setEscapeProcessing(boolean enable) throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return null;
	}

	@Override
	public void clearWarnings() throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	@Override
	public void setCursorName(String name) throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	@Override
	public int getUpdateCount() throws SQLException {
		return 0;
	}

	@Override
	public int getFetchDirection() throws SQLException {
		return 0;
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		// no-op
	}

	@Override
	public int getFetchSize() throws SQLException {
		return 0;
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	@Override
	public int getResultSetConcurrency() throws SQLException {
		return 0;
	}

	@Override
	public int getResultSetType() throws SQLException {
		return 0;
	}

	@Override
	public void addBatch(String sql) throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	@Override
	public void clearBatch() throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	@Override
	public int[] executeBatch() throws SQLException {
		return new int[0];
	}

	@Override
	public ResultSet getGeneratedKeys() throws SQLException {
		return null;
	}

	@Override
	public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
		return 0;
	}

	@Override
	public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
		return 0;
	}

	@Override
	public int executeUpdate(String sql, String[] columnNames) throws SQLException {
		return 0;
	}

	@Override
	public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
		return false;
	}

	@Override
	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		return false;
	}

	@Override
	public boolean execute(String sql, String[] columnNames) throws SQLException {
		return false;
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		return 0;
	}

	@Override
	public boolean isClosed() throws SQLException {
		return false;
	}

	@Override
	public boolean isPoolable() throws SQLException {
		return false;
	}

	@Override
	public void setPoolable(boolean poolable) throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	@Override
	public void closeOnCompletion() throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	@Override
	public boolean isCloseOnCompletion() throws SQLException {
		return false;
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return null;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return false;
	}
}