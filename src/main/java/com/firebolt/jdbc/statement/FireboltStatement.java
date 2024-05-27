package com.firebolt.jdbc.statement;

import com.firebolt.jdbc.JdbcBase;
import com.firebolt.jdbc.annotation.NotImplemented;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.ExceptionType;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.exception.FireboltSQLFeatureNotSupportedException;
import com.firebolt.jdbc.exception.FireboltUnsupportedOperationException;
import com.firebolt.jdbc.service.FireboltStatementService;
import com.firebolt.jdbc.util.CloseableUtil;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory.createValidator;
import static java.lang.String.format;
import static java.util.stream.Collectors.toCollection;

public class FireboltStatement extends JdbcBase implements Statement {

	private static final Logger log = Logger.getLogger(FireboltStatement.class.getName());
	private final FireboltStatementService statementService;
	private final FireboltProperties sessionProperties;
	private final FireboltConnection connection;
	private final Collection<String> statementsToExecuteLabels = new HashSet<>();
	private boolean closeOnCompletion = false;
	private int currentUpdateCount = -1;
	private int maxRows;
	private int maxFieldSize;
	private volatile boolean isClosed = false;
	private StatementResultWrapper currentStatementResult;
	private StatementResultWrapper firstUnclosedStatementResult;
	private int queryTimeout = 0; // zero means that there is no limit
	private String runningStatementLabel;
	private final List<String> batchStatements = new LinkedList<>();

	public FireboltStatement(FireboltStatementService statementService, FireboltProperties sessionProperties,
			FireboltConnection connection) {
		this.statementService = statementService;
		this.sessionProperties = sessionProperties;
		this.connection = connection;
		log.fine("Created Statement");
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
					resultSet = execute(statements.get(i));
				} else {
					execute(statements.get(i));
				}
			}
		} finally {
			synchronized (statementsToExecuteLabels) {
				statementsToExecuteLabels.removeAll(queryLabels);
			}
		}
		return resultSet;
	}

	@SuppressWarnings("java:S2139") // TODO: Exceptions should be either logged or rethrown but not both
	private Optional<ResultSet> execute(StatementInfoWrapper statementInfoWrapper) throws SQLException {
		createValidator(statementInfoWrapper.getInitialStatement(), connection).validate(statementInfoWrapper.getInitialStatement());
		ResultSet resultSet = null;
		if (isStatementNotCancelled(statementInfoWrapper)) {
			runningStatementLabel = statementInfoWrapper.getLabel();
			synchronized (this) {
				validateStatementIsNotClosed();
			}
			InputStream inputStream = null;
			try {
				log.log(Level.INFO, "Executing the statement with label {0} : {1}", new Object[] {statementInfoWrapper.getLabel(), statementInfoWrapper.getSql()});
				if (statementInfoWrapper.getType() == StatementType.PARAM_SETTING) {
					connection.addProperty(statementInfoWrapper.getParam());
					log.log(Level.FINE, "The property from the query {0} was stored", runningStatementLabel);
				} else {
					Optional<ResultSet> currentRs = statementService.execute(statementInfoWrapper, sessionProperties, this);
					if (currentRs.isPresent()) {
						resultSet = currentRs.get();
						currentUpdateCount = -1; // Always -1 when returning a ResultSet
					} else {
						currentUpdateCount = 0;
					}
					log.log(Level.INFO, "The query with the label {0} was executed with success", runningStatementLabel);
				}
			} catch (Exception ex) {
				CloseableUtil.close(inputStream);
				log.log(Level.SEVERE, ex, () -> format("An error happened while executing the statement with the id %s", runningStatementLabel));
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
			log.log(Level.FINE, "Aborted query with id {0}", statementInfoWrapper.getLabel());
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
			log.log(Level.INFO, "Cancelling statement with label {0}", statementLabel);
			abortStatementRunningOnFirebolt(statementLabel);
		}
	}

	private void abortStatementRunningOnFirebolt(String statementLabel) throws SQLException {
		try {
			statementService.abortStatement(statementLabel, sessionProperties);
			log.log(Level.FINE, "Statement with label {0} was aborted", statementLabel);
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
			throw new FireboltException(format("Illegal maxRows value: %d", max));
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
		log.fine("Statement closed");
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
	public int getMaxFieldSize() throws SQLException {
		return maxFieldSize;
	}

	@Override
	public void setMaxFieldSize(int max) throws SQLException {
		maxFieldSize = max;
	}

	@Override
	public void setEscapeProcessing(boolean enable) {
		if (enable) {
			addWarning(new SQLWarning("Escape processing is not supported right now", "0A000")); // see https://en.wikipedia.org/wiki/SQLSTATE
		}
	}

	@Override
	@NotImplemented
	public void setCursorName(String name) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	public int getFetchDirection() {
		return ResultSet.FETCH_FORWARD;
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		if (direction != ResultSet.FETCH_FORWARD) {
			throw new FireboltException(ExceptionType.TYPE_NOT_SUPPORTED);
		}
	}

	@Override
	public int getFetchSize() throws SQLException {
		return 0; // fetch size is not supported; 0 means unlimited like in PostgreSQL and MySQL
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
	public void addBatch(String sql) throws SQLException {
		batchStatements.add(sql);
	}

	@Override
	public void clearBatch() throws SQLException {
		batchStatements.clear();
	}

	@Override
	public int[] executeBatch() throws SQLException {
		List<Integer> result = new ArrayList<>();
		for (String sql : batchStatements) {
			for (StatementInfoWrapper query : StatementUtil.parseToStatementInfoWrappers(sql)) {
				@SuppressWarnings("java:S6912") // Use "addBatch" and "executeBatch" to execute multiple SQL statements in a single call - this is the implementation of executeBatch
				Optional<ResultSet> rs = execute(List.of(query));
				result.add(rs.map(x -> 0).orElse(SUCCESS_NO_INFO));
			}
		}
		return  result.stream().mapToInt(Integer::intValue).toArray();
	}

	@Override
	@NotImplemented
	public ResultSet getGeneratedKeys() throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
		if (autoGeneratedKeys != Statement.NO_GENERATED_KEYS) {
			throw new FireboltSQLFeatureNotSupportedException();
		}
		return executeUpdate(sql);
	}

	@Override
	public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
		if (columnIndexes == null || columnIndexes.length == 0) {
			return executeUpdate(sql);
		}
		throw new FireboltSQLFeatureNotSupportedException("Returning autogenerated keys by column index is not supported.");
	}

	@Override
	public int executeUpdate(String sql, String[] columnNames) throws SQLException {
		if (columnNames == null || columnNames.length == 0) {
			return executeUpdate(sql);
		}
		throw new FireboltSQLFeatureNotSupportedException("Returning autogenerated keys by column name is not supported.");
	}

	@Override
	public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
		if (autoGeneratedKeys != Statement.NO_GENERATED_KEYS) {
			throw new FireboltSQLFeatureNotSupportedException();
		}
		return execute(sql);
	}

	@Override
	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		if (columnIndexes == null || columnIndexes.length == 0) {
			return execute(sql);
		}
		throw new FireboltSQLFeatureNotSupportedException("Returning autogenerated keys by column index is not supported.");
	}

	@Override
	public boolean execute(String sql, String[] columnNames) throws SQLException {
		if (columnNames == null || columnNames.length == 0) {
			return execute(sql);
		}
		throw new FireboltSQLFeatureNotSupportedException("Returning autogenerated keys by column name is not supported.");
	}

	@Override
	public int getResultSetHoldability() {
		// N/A applicable as we do not support transactions => commits do not affect anything => kind of hold cursors over commit
		return ResultSet.HOLD_CURSORS_OVER_COMMIT;
	}

	@Override
	public boolean isPoolable() {
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
