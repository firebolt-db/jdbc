package com.firebolt.jdbc.statement;

import com.firebolt.jdbc.annotation.ExcludeFromJacocoGeneratedReport;
import com.firebolt.jdbc.annotation.NotImplemented;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.exception.FireboltSQLFeatureNotSupportedException;
import com.firebolt.jdbc.exception.FireboltUnsupportedOperationException;
import com.firebolt.jdbc.service.FireboltStatementService;
import com.firebolt.jdbc.util.CloseableUtil;
import lombok.CustomLog;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static java.util.stream.Collectors.toCollection;

@CustomLog
public class FireboltStatement implements Statement {

	private final FireboltStatementService statementService;
	private final FireboltProperties sessionProperties;
	private final FireboltConnection connection;
	private final Collection<String> statementsToExecuteLabels = new HashSet<>();
	private boolean closeOnCompletion = false;
	private int currentUpdateCount = -1;
	private int maxRows;
	private volatile boolean isClosed = false;
	private StatementResultWrapper currentStatementResult;
	private StatementResultWrapper firstUnclosedStatementResult;
	private int queryTimeout = 0; // zero means that there is no limit
	private String runningStatementLabel;

	public FireboltStatement(FireboltStatementService statementService, FireboltProperties sessionProperties,
			FireboltConnection connection) {
		this.statementService = statementService;
		this.sessionProperties = sessionProperties;
		this.connection = connection;
		log.debug("Created Statement");
	}

	@Override
	public ResultSet executeQuery(String sql) throws SQLException {
		return executeQuery(StatementUtil.parseToStatementInfoWrappers(sql));
	}

	protected ResultSet executeQuery(List<StatementInfoWrapper> statementInfoList) throws SQLException {
		StatementInfoWrapper query = getOneQueryStatementInfo(statementInfoList);
		Optional<ResultSet> resultSet = execute(Collections.singletonList(query));
		synchronized (this) {
			return resultSet.orElseThrow(() -> new FireboltException("Could not return ResultSet - the query returned no result."));
		}
	}

	@Override
	public boolean execute(String sql) throws SQLException {
		return execute(StatementUtil.parseToStatementInfoWrappers(sql)).isPresent();
	}

	protected Optional<ResultSet> execute(List<StatementInfoWrapper> statements) throws SQLException {
		Optional<ResultSet> resultSet = Optional.empty();
		closeAllResults();
		Set<String> queryLabels = statements.stream().map(StatementInfoWrapper::getLabel).collect(toCollection(HashSet::new));
		try {
			synchronized (statementsToExecuteLabels) {
				statementsToExecuteLabels.addAll(queryLabels);
			}
			for (int i = 0; i < statements.size(); i++) {
				if (i == 0) {
					resultSet = execute(statements.get(i), true, true);
				} else {
					execute(statements.get(i), true, true);
				}
			}
		} finally {
			synchronized (statementsToExecuteLabels) {
				statementsToExecuteLabels.removeAll(queryLabels);
			}
		}
		return resultSet;
	}

	private Optional<ResultSet> execute(StatementInfoWrapper statementInfoWrapper, boolean verifyNotCancelled, boolean isStandardSql) throws SQLException {
		ResultSet resultSet = null;
		if (!verifyNotCancelled || isStatementNotCancelled(statementInfoWrapper)) {
			runningStatementLabel = statementInfoWrapper.getLabel();
			synchronized (this) {
				validateStatementIsNotClosed();
			}
			InputStream inputStream = null;
			try {
				log.info("Executing the statement with label {} : {}", statementInfoWrapper.getLabel(),
						statementInfoWrapper.getSql());
				if (statementInfoWrapper.getType() == StatementType.PARAM_SETTING) {
					connection.addProperty(statementInfoWrapper.getParam());
					log.debug("The property from the query {} was stored", runningStatementLabel);
				} else {
					Optional<ResultSet> currentRs = statementService.execute(statementInfoWrapper,
							sessionProperties, queryTimeout, maxRows, isStandardSql, sessionProperties.isSystemEngine(), this);
					if (currentRs.isPresent()) {
						resultSet = currentRs.get();
						currentUpdateCount = -1; // Always -1 when returning a ResultSet
					} else {
						currentUpdateCount = 0;
					}
					log.info("The query with the label {} was executed with success", runningStatementLabel);
				}
			} catch (Exception ex) {
				CloseableUtil.close(inputStream);
				log.error(String.format("An error happened while executing the statement with the id %s",
						runningStatementLabel), ex);
				throw ex;
			} finally {
				runningStatementLabel = null;
			}
			synchronized (this) {
				if (firstUnclosedStatementResult == null) {
					firstUnclosedStatementResult = currentStatementResult = new StatementResultWrapper(resultSet, statementInfoWrapper);
				} else {
					firstUnclosedStatementResult.append(new StatementResultWrapper(resultSet, statementInfoWrapper));
				}
			}
		} else {
			log.warn("Aborted query with id {}", statementInfoWrapper.getLabel());
		}
		return Optional.ofNullable(resultSet);
	}

	private boolean isStatementNotCancelled(StatementInfoWrapper statementInfoWrapper) {
		synchronized (statementsToExecuteLabels) {
			return statementsToExecuteLabels.contains(statementInfoWrapper.getLabel());
		}
	}

	private void closeAllResults() {
		synchronized (this) {
			if (firstUnclosedStatementResult != null) {
				firstUnclosedStatementResult.close();
				firstUnclosedStatementResult = null;
			}
		}
	}

	@Override
	public void cancel() throws SQLException {
		synchronized (statementsToExecuteLabels) {
			statementsToExecuteLabels.clear();
		}
		String statementLabel = runningStatementLabel;
		if (statementLabel != null) {
			log.info("Cancelling statement with label " + statementLabel);
			abortStatementRunningOnFirebolt(statementLabel);
		}
	}

	private void abortStatementRunningOnFirebolt(String statementLabel) throws SQLException {
		try {
			statementService.abortStatement(statementLabel, sessionProperties);
			log.debug("Statement with label {} was aborted", statementLabel);
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
		return executeUpdate(StatementUtil.parseToStatementInfoWrappers(sql));
	}

	protected int executeUpdate(List<StatementInfoWrapper> sql) throws SQLException {
		execute(sql);
		StatementResultWrapper response;
		synchronized (this) {
			response = firstUnclosedStatementResult;
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
		return connection;
	}

	@Override
	public boolean getMoreResults(int current) throws SQLException {
		synchronized (this) {
			validateStatementIsNotClosed();

			if (current == Statement.CLOSE_CURRENT_RESULT && currentStatementResult != null
					&& currentStatementResult.getResultSet() != null) {
				currentStatementResult.getResultSet().close();
			}

			if (currentStatementResult != null) {
				currentStatementResult = currentStatementResult.getNext();
			}

			if (current == Statement.CLOSE_ALL_RESULTS) {
				closeUnclosedProcessedResults();
			}

			return (currentStatementResult != null && currentStatementResult.getResultSet() != null);
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
		closeAllResults();

		if (removeFromConnection && connection != null) {
			connection.removeClosedStatement(this);
		}
		cancel();
		log.debug("Statement closed");
	}

	@Override
	public boolean isClosed() throws SQLException {
		return isClosed;
	}

	@Override
	public synchronized ResultSet getResultSet() throws SQLException {
		return firstUnclosedStatementResult != null ? firstUnclosedStatementResult.getResultSet() : null;
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
		return runningStatementLabel != null && statementService.isStatementRunning(runningStatementLabel);
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
	public SQLWarning getWarnings() throws SQLException {
		return null;
	}

	@Override
	@NotImplemented
	public void clearWarnings() throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	@Override
	@NotImplemented
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
	public int getFetchSize() throws SQLException {
		return 0;
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		validateStatementIsNotClosed();
		if (rows < 0) {
			throw new SQLException("The number of rows cannot be less than 0");
		}
		// Ignore
	}

	@Override
	public int getResultSetConcurrency() {
		return ResultSet.CONCUR_READ_ONLY;
	}

	@Override
	public int getResultSetType() {
		return ResultSet.TYPE_FORWARD_ONLY;
	}

	@Override
	@NotImplemented
	public void addBatch(String sql) throws SQLException {
		// Batch are not supported by the driver
		throw new FireboltUnsupportedOperationException();
	}

	@Override
	@NotImplemented
	public void clearBatch() throws SQLException {
		// Batch are not supported by the driver
		throw new FireboltUnsupportedOperationException();
	}

	@Override
	@NotImplemented
	public int[] executeBatch() throws SQLException {
		// Batch are not supported by the driver
		throw new FireboltUnsupportedOperationException();
	}

	@Override
	@NotImplemented
	public ResultSet getGeneratedKeys() throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public int executeUpdate(String sql, String[] columnNames) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public boolean execute(String sql, String[] columnNames) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public int getResultSetHoldability() throws SQLException {
		// N/A applicable as we do not support transactions => commits do not affect anything => kind of hold cursors over commit
		return ResultSet.HOLD_CURSORS_OVER_COMMIT;
	}

	@Override
	public boolean isPoolable() throws SQLException {
		return false;
	}

	@Override
	@NotImplemented
	public void setPoolable(boolean poolable) throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	/**
	 * Returns true if the statement has more results
	 *
	 * @return true if the statement has more results
	 */
	public boolean hasMoreResults() {
		return currentStatementResult.getNext() != null;
	}
}
