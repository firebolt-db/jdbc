package com.firebolt.jdbc.statement;

import java.io.InputStream;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import com.firebolt.jdbc.annotation.ExcludeFromJacocoGeneratedReport;
import com.firebolt.jdbc.annotation.NotImplemented;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.exception.FireboltSQLFeatureNotSupportedException;
import com.firebolt.jdbc.exception.FireboltUnsupportedOperationException;
import com.firebolt.jdbc.service.FireboltStatementService;
import com.firebolt.jdbc.util.CloseableUtil;
import com.firebolt.jdbc.util.PropertyUtil;

import lombok.Builder;
import lombok.CustomLog;

@CustomLog
public class FireboltStatement implements Statement {

	private final FireboltStatementService statementService;
	private final FireboltProperties sessionProperties;
	private final FireboltConnection connection;
	private final Collection<String> statementsToExecuteIds = new HashSet<>();
	private boolean closeOnCompletion = false;
	private int currentUpdateCount = -1;
	private int maxRows;
	private volatile boolean isClosed = false;
	private StatementResultWrapper currentStatementResult;
	private StatementResultWrapper firstUnclosedStatementResult;
	private int queryTimeout = -1;
	private String runningStatementId;

	@Builder
	public FireboltStatement(FireboltStatementService statementService, FireboltProperties sessionProperties,
			FireboltConnection connection) {
		this.statementService = statementService;
		this.sessionProperties = sessionProperties;
		this.connection = connection;
		log.debug("Created Statement");
	}

	@Override
	public ResultSet executeQuery(String sql) throws SQLException {
		return this.executeQuery(StatementUtil.parseToStatementInfoWrappers(sql));
	}

	protected ResultSet executeQuery(List<StatementInfoWrapper> statementInfoList) throws SQLException {
		StatementInfoWrapper query = getOneQueryStatementInfo(statementInfoList);
		Optional<ResultSet> resultSet = this.execute(Collections.singletonList(query));
		synchronized (this) {
			if (!resultSet.isPresent()) {
				throw new FireboltException("Could not return ResultSet - the query returned no result.");
			} else {
				return resultSet.get();
			}
		}
	}

	@Override
	public boolean execute(String sql) throws SQLException {
		return this.execute(StatementUtil.parseToStatementInfoWrappers(sql)).isPresent();
	}

	protected Optional<ResultSet> execute(List<StatementInfoWrapper> statements) throws SQLException {
		Optional<ResultSet> resultSet = Optional.empty();
		this.closeAllResults();
		Set<String> queryIds = statements.stream().map(StatementInfoWrapper::getId)
				.collect(Collectors.toCollection(HashSet::new));
		try {
			synchronized (statementsToExecuteIds) {
				statementsToExecuteIds.addAll(queryIds);
			}
			for (int i = 0; i < statements.size(); i++) {
				if (i == 0) {
					resultSet = execute(statements.get(i), true, true);
				} else {
					execute(statements.get(i), true, true);
				}
			}
		} finally {
			synchronized (statementsToExecuteIds) {
				statementsToExecuteIds.removeAll(queryIds);
			}
		}
		return resultSet;
	}

	private Optional<ResultSet> execute(StatementInfoWrapper statementInfoWrapper, boolean verifyNotCancelled,
			boolean isStandardSql) throws SQLException {
		ResultSet resultSet = null;
		if (!verifyNotCancelled || isStatementNotCancelled(statementInfoWrapper)) {
			runningStatementId = statementInfoWrapper.getId();
			synchronized (this) {
				this.validateStatementIsNotClosed();
			}
			InputStream inputStream = null;
			try {
				log.info("Executing the statement with id {} : {}", statementInfoWrapper.getId(),
						statementInfoWrapper.getSql());
				if (statementInfoWrapper.getType() == StatementType.PARAM_SETTING) {
					this.connection.addProperty(statementInfoWrapper.getParam());
					log.debug("The property from the query {} was stored", runningStatementId);
				} else {
					Optional<ResultSet> currentRs = statementService.execute(statementInfoWrapper,
							this.sessionProperties, this.queryTimeout, this.maxRows, isStandardSql, sessionProperties.isSystemEngine(), this);
					if (currentRs.isPresent()) {
						resultSet = currentRs.get();
						currentUpdateCount = -1; // Always -1 when returning a ResultSet
					} else {
						currentUpdateCount = 0;
					}
					log.info("The query with the id {} was executed with success", runningStatementId);
				}
			} catch (Exception ex) {
				CloseableUtil.close(inputStream);
				log.error(String.format("An error happened while executing the statement with the id %s",
						runningStatementId), ex);
				throw ex;
			} finally {
				runningStatementId = null;
			}
			synchronized (this) {
				if (this.firstUnclosedStatementResult == null) {
					this.firstUnclosedStatementResult = this.currentStatementResult = new StatementResultWrapper(
							resultSet, statementInfoWrapper);
				} else {
					this.firstUnclosedStatementResult
							.append(new StatementResultWrapper(resultSet, statementInfoWrapper));
				}
			}
		} else {
			log.warn("Aborted query with id {}", statementInfoWrapper.getId());
		}
		return Optional.ofNullable(resultSet);
	}

	private boolean isStatementNotCancelled(StatementInfoWrapper statementInfoWrapper) {
		synchronized (statementsToExecuteIds) {
			return statementsToExecuteIds.contains(statementInfoWrapper.getId());
		}
	}

	private void closeAllResults() {
		synchronized (this) {
			if (this.firstUnclosedStatementResult != null) {
				this.firstUnclosedStatementResult.close();
				this.firstUnclosedStatementResult = null;
			}
		}
	}

	@Override
	public void cancel() throws SQLException {
		synchronized (statementsToExecuteIds) {
			statementsToExecuteIds.clear();
		}
		String statementId = runningStatementId;
		if (statementId != null) {
			log.info("Cancelling statement with id " + statementId);
			try {
				statementService.abortStatementHttpRequest(statementId);
			} finally {
				abortStatementRunningOnFirebolt(statementId);
			}
		}
	}

	private void abortStatementRunningOnFirebolt(String statementId) throws SQLException {
		try {
			statementService.abortStatement(statementId, this.sessionProperties);
			log.debug("Statement with id {} was aborted", statementId);
		} catch (FireboltException e) {
			throw e;
		} catch (Exception e) {
			throw new FireboltException("Could not abort statement", e);
		} finally {
			synchronized (connection) {
				connection.notifyAll();
			}
		}
	}

	@Override
	public int executeUpdate(String sql) throws SQLException {
		return this.executeUpdate(StatementUtil.parseToStatementInfoWrappers(sql));
	}

	protected int executeUpdate(List<StatementInfoWrapper> sql) throws SQLException {
		this.execute(sql);
		StatementResultWrapper response;
		synchronized (this) {
			response = this.firstUnclosedStatementResult;
		}
		try {
			while (response != null && response.getResultSet() != null) {
				response = response.getNext();
				if (response.getResultSet() != null) {
					throw new FireboltException("A ResulSet was returned although none was expected");
				}
			}
		} finally {
			closeAllResults();
		}
		return 0;
	}

	@Override
	public Connection getConnection() {
		return this.connection;
	}

	@Override
	public boolean getMoreResults(int current) throws SQLException {
		synchronized (this) {
			validateStatementIsNotClosed();

			if (current == Statement.CLOSE_CURRENT_RESULT && this.currentStatementResult != null
					&& this.currentStatementResult.getResultSet() != null) {
				this.currentStatementResult.getResultSet().close();
			}

			if (this.currentStatementResult != null) {
				this.currentStatementResult = this.currentStatementResult.getNext();
			}

			if (current == Statement.CLOSE_ALL_RESULTS) {
				closeUnclosedProcessedResults();
			}

			return (this.currentStatementResult != null && this.currentStatementResult.getResultSet() != null);
		}
	}

	private synchronized void closeUnclosedProcessedResults() throws SQLException {
		StatementResultWrapper responseWrapper = firstUnclosedStatementResult;
		while (responseWrapper != currentStatementResult && responseWrapper != null) {
			if (responseWrapper.getResultSet() != null) {
				responseWrapper.getResultSet().close();
			}
			responseWrapper = responseWrapper.getNext();
		}
		firstUnclosedStatementResult = responseWrapper;
	}

	@Override
	public int getMaxRows() throws SQLException {
		return maxRows;
	}

	@Override
	public void setMaxRows(int max) throws SQLException {
		if (max < 0) {
			throw new FireboltException(String.format("Illegal maxRows value: %d", max));
		}
		maxRows = max;
	}

	@Override
	public void close() throws SQLException {
		close(true);
	}

	/**
	 * Closes the Statement and removes the object from the list of Statements kept
	 * in the {@link FireboltConnection} if the param removeFromConnection is set to
	 * true
	 *
	 * @param removeFromConnection whether the {@link FireboltStatement} must be
	 *                             removed from the parent
	 *                             {@link FireboltConnection}
	 */
	public void close(boolean removeFromConnection) throws SQLException {
		synchronized (this) {
			if (isClosed) {
				return;
			}
			isClosed = true;
		}
		this.closeAllResults();

		if (removeFromConnection) {
			connection.removeClosedStatement(this);
		}
		cancel();
		log.debug("Statement closed");
	}

	@Override
	public boolean isClosed() throws SQLException {
		return this.isClosed;
	}

	@Override
	public synchronized ResultSet getResultSet() throws SQLException {
		return this.firstUnclosedStatementResult != null ? this.firstUnclosedStatementResult.getResultSet() : null;
	}

	@Override
	public boolean getMoreResults() throws SQLException {
		boolean result = getMoreResults(CLOSE_ALL_RESULTS);
		if (!result) {
			currentUpdateCount = -1;
		}
		return result;
	}

	@Override
	public int getUpdateCount() throws SQLException {
		return currentUpdateCount;
	}

	@Override
	public void closeOnCompletion() throws SQLException {
		closeOnCompletion = true;
	}

	@Override
	public boolean isCloseOnCompletion() throws SQLException {
		return closeOnCompletion;
	}

	@Override
	public int getQueryTimeout() throws SQLException {
		return queryTimeout;
	}

	@Override
	public void setQueryTimeout(int seconds) throws SQLException {
		queryTimeout = seconds;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) {
		return iface.isAssignableFrom(getClass());
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		if (iface.isAssignableFrom(getClass())) {
			return iface.cast(this);
		}
		throw new SQLException("Cannot unwrap to " + iface.getName());
	}

	protected void validateStatementIsNotClosed() throws SQLException {
		if (isClosed()) {
			throw new FireboltException("Cannot proceed: statement closed");
		}
	}

	protected StatementInfoWrapper getOneQueryStatementInfo(List<StatementInfoWrapper> statementInfoList)
			throws SQLException {
		if (statementInfoList.size() != 1 || statementInfoList.get(0).getType() != StatementType.QUERY) {
			throw new FireboltException("Cannot proceed: the statement would not return a ResultSet");
		} else {
			return statementInfoList.get(0);
		}
	}

	/**
	 * Returns true if the statement is currently running
	 *
	 * @return true if the statement is currently running
	 */
	public boolean isStatementRunning() {
		return this.runningStatementId != null && statementService.isStatementRunning(this.runningStatementId);
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public int getMaxFieldSize() throws SQLException {
		return 0;
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void setMaxFieldSize(int max) throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void setEscapeProcessing(boolean enable) throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public SQLWarning getWarnings() throws SQLException {
		return null;
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void clearWarnings() throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void setCursorName(String name) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public int getFetchDirection() throws SQLException {
		return ResultSet.FETCH_FORWARD;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public void setFetchDirection(int direction) throws SQLException {
		// no-op
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public int getFetchSize() throws SQLException {
		return 0;
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		validateStatementIsNotClosed();
		if (rows < 0) {
			throw new IllegalArgumentException("The number of rows cannot be less than 0");
		}
		// Ignore
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public int getResultSetConcurrency() throws SQLException {
		return 0;
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public int getResultSetType() throws SQLException {
		return 0;
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void addBatch(String sql) throws SQLException {
		// Batch are not supported by the driver
		throw new FireboltUnsupportedOperationException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void clearBatch() throws SQLException {
		// Batch are not supported by the driver
		throw new FireboltUnsupportedOperationException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public int[] executeBatch() throws SQLException {
		// Batch are not supported by the driver
		throw new FireboltUnsupportedOperationException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public ResultSet getGeneratedKeys() throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public int executeUpdate(String sql, String[] columnNames) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public boolean execute(String sql, String[] columnNames) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public int getResultSetHoldability() throws SQLException {
		// N/A applicable as we do not support transactions
		return 0;
	}

	@Override
	public boolean isPoolable() throws SQLException {
		return false;
	}

	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void setPoolable(boolean poolable) throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	/**
	 * Returns true if the statement has more results
	 *
	 * @return true if the statement has more results
	 */
	public boolean hasMoreResults() {
		return this.currentStatementResult.getNext() != null;
	}
}
