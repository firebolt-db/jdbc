package com.firebolt.jdbc.statement;

import java.io.InputStream;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

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

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FireboltStatement implements Statement {

	private static final String KILL_QUERY_SQL = "KILL QUERY ON CLUSTER sql_cluster WHERE initial_query_id='%s'";
	private final FireboltStatementService statementService;
	private final FireboltProperties sessionProperties;
	private final FireboltConnection connection;
	private boolean closeOnCompletion = false;
	private int currentUpdateCount = -1;
	private int maxRows;
	private volatile boolean isClosed = false;
	private StatementResponseWrapper statementResponseWrapper;
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
		validateStatementWillReturnAResultSet(sql);
		this.execute(sql, (Map<String, String>) null);
		return statementResponseWrapper.getResultSet();
	}

	private void execute(String sql, Map<String, String> params) throws SQLException {
		if (this.statementResponseWrapper != null) {
			this.statementResponseWrapper.close();
			this.statementResponseWrapper = null;
			log.info("There was already an opened ResultSet for the statement object. The ResultSet is now closed.");
		}
		List<StatementInfoWrapper> queries = StatementUtil.getQueryWrapper(sql).getSubQueries().stream().map(QueryWrapper.SubQuery::getSql)
				.map(StatementUtil::extractStatementInfo).collect(Collectors.toList());
		for (int i = 0; i < queries.size(); i++) {
			StatementInfoWrapper statementInfoWrapper = queries.get(i);
			ResultSet rs = execute(statementInfoWrapper, params);
			if (i == 0) {
				this.statementResponseWrapper = new StatementResponseWrapper(rs, statementInfoWrapper);
			} else {
				this.statementResponseWrapper.append(new StatementResponseWrapper(rs, statementInfoWrapper));
			}
		}
	}

	private ResultSet execute(StatementInfoWrapper statementInfo, Map<String, String> statementParams)
			throws SQLException {
		runningStatementId = UUID.randomUUID().toString();
		ResultSet resultSet = null;
		synchronized (this) {
			this.validateStatementIsNotClosed();
		}
		InputStream inputStream = null;
		try {
			log.info("Executing the statement with id {} : {}", statementInfo.getId(), statementInfo.getSql());
			if (statementInfo.getType() == StatementInfoWrapper.StatementType.PARAM_SETTING) {
				this.connection.addProperty(statementInfo.getParam());
				log.debug("The property from the query {} was stored", runningStatementId);
			} else {
				Map<String, String> params = statementParams != null ? statementParams : this.getStatementParameters();
				inputStream = statementService.execute(statementInfo, this.sessionProperties, params);
				if (statementInfo.getType() == StatementInfoWrapper.StatementType.QUERY) {
					currentUpdateCount = -1; // Always -1 when returning a ResultSet
					Pair<Optional<String>, Optional<String>> dbNameAndTableNamePair = StatementUtil
							.extractDbNameAndTableNamePairFromQuery(statementInfo.getSql());

					resultSet = new FireboltResultSet(inputStream, dbNameAndTableNamePair.getRight().orElse("unknown"),
							dbNameAndTableNamePair.getLeft().orElse(this.sessionProperties.getDatabase()),
							this.sessionProperties.getBufferSize(), this.sessionProperties.isCompress(), this,
							this.sessionProperties.isLogResultSet());
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
			this.runningStatementId = null;
		}
		return resultSet;
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
		if (runningStatementId != null) {
			log.debug("Cancelling statement with id {}", runningStatementId);
			try {
				statementService.abortStatementHttpRequest(runningStatementId);
			} finally {
				abortStatementRunningOnFirebolt(runningStatementId);
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
		validateStatementWillNotReturnAResultSet(sql);
		this.execute(sql);
		return 0;
	}

	@Override
	public Connection getConnection() throws SQLException {
		return this.connection;
	}

	@Override
	public boolean getMoreResults(int current) throws SQLException {
		return false;
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
		if (this.statementResponseWrapper != null) {
			this.statementResponseWrapper.close();
			this.statementResponseWrapper = null;
		}

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
	public ResultSet getResultSet() throws SQLException {
		return this.statementResponseWrapper != null ? this.statementResponseWrapper.getResultSet() : null;
	}

	@Override
	public boolean getMoreResults() throws SQLException {
		if (this.statementResponseWrapper != null) {
			if (this.statementResponseWrapper.getResultSet() != null) {
				this.statementResponseWrapper.getResultSet().close();
			}
			this.statementResponseWrapper = statementResponseWrapper.getNext();
			return true;
		} else {
			currentUpdateCount = -1;
			return false;
		}
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
		this.execute(sql, (Map<String, String>) null);
		return statementResponseWrapper.getStatementInfoWrapper().getType() == StatementInfoWrapper.StatementType.QUERY;
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

	protected void validateStatementWillReturnAResultSet(String sql) throws SQLException {
		List<StatementInfoWrapper> queries = StatementUtil.getQueryWrapper(sql).getSubQueries().stream().map(QueryWrapper.SubQuery::getSql)
				.map(StatementUtil::extractStatementInfo).collect(Collectors.toList());
		if (queries.size() != 1 || queries.get(0).getType() != StatementInfoWrapper.StatementType.QUERY) {
			throw new FireboltException("Cannot proceed: the statement would not return a ResultSet");
		}
	}

	protected void validateStatementWillNotReturnAResultSet(String sql) throws SQLException {
		if (StatementUtil.isQuery(sql)) {
			throw new FireboltException("Cannot proceed: the statement would return a ResultSet");
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

}
