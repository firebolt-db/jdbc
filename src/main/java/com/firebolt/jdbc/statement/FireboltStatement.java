package com.firebolt.jdbc.statement;

import java.io.InputStream;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import com.firebolt.jdbc.CloseableUtil;
import com.firebolt.jdbc.PropertyUtil;
import com.firebolt.jdbc.annotation.ExcludeFromJacocoGeneratedReport;
import com.firebolt.jdbc.annotation.NotImplemented;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.exception.FireboltSQLFeatureNotSupportedException;
import com.firebolt.jdbc.exception.FireboltUnsupportedOperationException;
import com.firebolt.jdbc.resultset.FireboltResultSet;
import com.firebolt.jdbc.service.FireboltStatementService;
import com.firebolt.jdbc.statement.rawstatement.QueryRawStatement;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FireboltStatement implements Statement {

	private static final String KILL_QUERY_SQL = "KILL QUERY ON CLUSTER sql_cluster WHERE initial_query_id='%s'";
	private final FireboltStatementService statementService;
	private final FireboltProperties sessionProperties;
	private final FireboltConnection connection;
	private final Collection<String> cancelledStatementIds = new HashSet<>();
	private final Collection<String> executedStatementsIds = new HashSet<>();
	private boolean closeOnCompletion = false;
	private int currentUpdateCount = -1;
	private int maxRows;
	private volatile boolean isClosed = false;
	private StatementResponseWrapper currentResult;
	private StatementResponseWrapper firstUnclosedResult;
	private Integer queryTimeout;
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

	public ResultSet executeQuery(List<StatementInfoWrapper> statementInfoWrappers) throws SQLException {
		StatementInfoWrapper query = getOneQueryStatementInfo(statementInfoWrappers);
		this.execute(Collections.singletonList(query), null);
		synchronized (this) {
			if (firstUnclosedResult == null) {
				throw new FireboltException("Could not return ResultSet - the result object is null");
			} else {
				return firstUnclosedResult.getResultSet();
			}
		}
	}

	private boolean execute(String sql, Map<String, String> params) throws SQLException {
		return this.execute(StatementUtil.parseToStatementInfoWrappers(sql), params);
	}

	private boolean execute(List<StatementInfoWrapper> statements, Map<String, String> params) throws SQLException {
		Boolean isQuery = null;
		this.closeAllResults();
		Set<String> queryIds = statements.stream().map(StatementInfoWrapper::getId)
				.collect(Collectors.toCollection(HashSet::new));
		try {
			synchronized (executedStatementsIds) {
				executedStatementsIds.addAll(queryIds);
			}
			for (StatementInfoWrapper statementInfoWrapper : statements) {
				if (!isStatementBeingCancelled(statementInfoWrapper)) {
					isQuery = isQuery == null ? execute(statementInfoWrapper, params) : isQuery;
				} else {
					log.info("Did not run cancelled statement with id {}", statementInfoWrapper.getId());
				}
			}
		} finally {
			synchronized (executedStatementsIds) {
				executedStatementsIds.removeAll(queryIds);
			}
		}
		return isQuery != null && isQuery;
	}

	private boolean execute(StatementInfoWrapper statementInfoWrapper, Map<String, String> params) throws SQLException {
		boolean isQuery = false;
		runningStatementId = statementInfoWrapper.getId();
		ResultSet rs = null;
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
				Map<String, String> statementParams = params != null ? params : this.getStatementParameters();
				inputStream = statementService.execute(statementInfoWrapper, this.sessionProperties, statementParams);
				if (statementInfoWrapper.getType() == StatementType.QUERY) {
					rs = getResultSet(inputStream, (QueryRawStatement) statementInfoWrapper.getInitialQuery());
					currentUpdateCount = -1; // Always -1 when returning a ResultSet
					isQuery = true;
				} else {
					currentUpdateCount = 0;
					CloseableUtil.close(inputStream);
				}
				log.info("The query with the id {} was executed with success", runningStatementId);
			}
		} catch (Exception ex) {
			CloseableUtil.close(inputStream);
			log.error("An error happened while executing the statement with the id {}", runningStatementId, ex);
			throw ex;
		} finally {
			runningStatementId = null;
			synchronized (executedStatementsIds) {
				executedStatementsIds.remove(statementInfoWrapper.getId());
			}
		}
		synchronized (this) {
			if (this.firstUnclosedResult == null) {
				this.firstUnclosedResult = this.currentResult = new StatementResponseWrapper(rs, statementInfoWrapper);
			} else {
				this.firstUnclosedResult.append(new StatementResponseWrapper(rs, statementInfoWrapper));
			}
		}
		return isQuery;
	}

	private boolean isStatementBeingCancelled(StatementInfoWrapper statementInfoWrapper) {
		synchronized (cancelledStatementIds) {
			return cancelledStatementIds.contains(statementInfoWrapper.getId());
		}
	}

	private FireboltResultSet getResultSet(InputStream inputStream, QueryRawStatement initialQuery)
			throws SQLException {
		return new FireboltResultSet(inputStream, Optional.ofNullable(initialQuery.getTable()).orElse("unknown"),
				Optional.ofNullable(initialQuery.getDatabase()).orElse(this.sessionProperties.getDatabase()),
				this.sessionProperties.getBufferSize(), this.sessionProperties.isCompress(), this,
				this.sessionProperties.isLogResultSet());
	}

	private void closeAllResults() {
		synchronized (this) {
			if (this.firstUnclosedResult != null) {
				this.firstUnclosedResult.close();
				this.firstUnclosedResult = null;
			}
		}
	}

	private Map<String, String> getStatementParameters() {
		Map<String, String> params = new HashMap<>();
		if (this.queryTimeout != null) {
			params.put("max_execution_time", String.valueOf(this.queryTimeout));
		}
		if (maxRows > 0) {
			params.put("max_result_rows", String.valueOf(this.maxRows));
			params.put("result_overflow_mode", "break");
		}
		return params;
	}

	@Override
	public void cancel() throws SQLException {
		synchronized (cancelledStatementIds) {
			cancelledStatementIds.clear();
			synchronized (executedStatementsIds) {
				cancelledStatementIds.addAll(executedStatementsIds);
			}
		}
		String statementId = runningStatementId;
		if (statementId != null) {
			System.out.println("Cancelling statement with id " + statementId);
			log.info("Cancelling statement with id {}", statementId);
			try {
				statementService.abortStatementHttpRequest(statementId);
			} finally {
				abortStatementRunningOnFirebolt(statementId);
			}
		}
	}

	private void abortStatementRunningOnFirebolt(String statementId) throws SQLException {
		try {
			if (PropertyUtil.isLocalDb(this.sessionProperties)
					|| StringUtils.isEmpty(this.sessionProperties.getDatabase())
					|| this.sessionProperties.isAggressiveCancel()) {
				abortStatementByQuery(statementId);
			} else {
				statementService.abortStatement(statementId, this.sessionProperties);
			}
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

	private void abortStatementByQuery(String statementId) throws SQLException {
		Map<String, String> statementParams = new HashMap<>(this.getStatementParameters());
		statementParams.put("use_standard_sql", "0");
		this.execute(String.format(KILL_QUERY_SQL, statementId), statementParams);
	}

	@Override
	public int executeUpdate(String sql) throws SQLException {
		return this.executeUpdate(StatementUtil.parseToStatementInfoWrappers(sql));
	}

	public int executeUpdate(List<StatementInfoWrapper> sql) throws SQLException {
		this.execute(sql);
		StatementResponseWrapper response;
		synchronized (this) {
			response = this.firstUnclosedResult;
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

			if (current == Statement.CLOSE_CURRENT_RESULT && this.currentResult != null
					&& this.currentResult.getResultSet() != null) {
				this.currentResult.getResultSet().close();
			}

			if (this.currentResult != null) {
				this.currentResult = this.currentResult.getNext();
			}

			if (current == Statement.CLOSE_ALL_RESULTS) {
				closeUnclosedProcessedResults();
			}

			return (this.currentResult != null && this.currentResult.getResultSet() != null);
		}
	}

	private synchronized void closeUnclosedProcessedResults() throws SQLException {
		StatementResponseWrapper responseWrapper = firstUnclosedResult;
		while (responseWrapper != currentResult && responseWrapper != null) {
			if (responseWrapper.getResultSet() != null) {
				responseWrapper.getResultSet().close();
			}
			responseWrapper = responseWrapper.getNext();
		}
		firstUnclosedResult = responseWrapper;
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
		return this.firstUnclosedResult != null ? this.firstUnclosedResult.getResultSet() : null;
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
	public boolean execute(String sql) throws SQLException {
		return this.execute(sql, (Map<String, String>) null);
	}

	public boolean execute(List<StatementInfoWrapper> statementInfoWrappers) throws SQLException {
		return this.execute(statementInfoWrappers, null);
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

	protected StatementInfoWrapper getOneQueryStatementInfo(List<StatementInfoWrapper> statementInfoWrappers)
			throws SQLException {
		if (statementInfoWrappers.size() != 1 || statementInfoWrappers.get(0).getType() != StatementType.QUERY) {
			throw new FireboltException("Cannot proceed: the statement would not return a ResultSet");
		} else {
			return statementInfoWrappers.get(0);
		}
	}

	public boolean isStatementRunning() {
		return this.runningStatementId != null && statementService.isStatementRunning(this.runningStatementId);
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public int getMaxFieldSize() throws SQLException {
		return 0;
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void setMaxFieldSize(int max) throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void setEscapeProcessing(boolean enable) throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public SQLWarning getWarnings() throws SQLException {
		return null;
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void clearWarnings() throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	/**
	 * @hidden
	 */
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

	/**
	 * @hidden
	 */
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

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public int getResultSetConcurrency() throws SQLException {
		return 0;
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public int getResultSetType() throws SQLException {
		return 0;
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void addBatch(String sql) throws SQLException {
		// Batch are not supported by the driver
		throw new FireboltUnsupportedOperationException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void clearBatch() throws SQLException {
		// Batch are not supported by the driver
		throw new FireboltUnsupportedOperationException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public int[] executeBatch() throws SQLException {
		// Batch are not supported by the driver
		throw new FireboltUnsupportedOperationException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public ResultSet getGeneratedKeys() throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public int executeUpdate(String sql, String[] columnNames) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public boolean execute(String sql, String[] columnNames) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	/**
	 * @hidden
	 */
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

	/**
	 * @hidden
	 */
	@Override
	@NotImplemented
	@ExcludeFromJacocoGeneratedReport
	public void setPoolable(boolean poolable) throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	public boolean hasMoreResults() {
		return this.currentResult.getNext() != null;
	}
}
