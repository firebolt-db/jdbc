package com.firebolt.jdbc.statement;

import com.firebolt.jdbc.CloseableUtils;
import com.firebolt.jdbc.PropertyUtil;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.resultset.FireboltResultSet;
import com.firebolt.jdbc.service.FireboltStatementService;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Slf4j
public class FireboltStatement extends AbstractStatement {

  private static final String KILL_QUERY_SQL =
      "KILL QUERY ON CLUSTER sql_cluster WHERE initial_query_id='%s'";
  private final FireboltStatementService statementService;
  private final FireboltProperties sessionProperties;
  private String runningStatementId;
  private boolean closeOnCompletion = true;
  private int currentUpdateCount = -1;
  private int maxRows;
  private volatile boolean isClosed = false;

  private ResultSet resultSet;

  private final FireboltConnection connection;

  private Integer queryTimeout;

  @Builder
  public FireboltStatement(
      FireboltStatementService statementService,
      FireboltProperties sessionProperties,
      FireboltConnection connection) {
    this.statementService = statementService;
    this.sessionProperties = sessionProperties;
    this.connection = connection;
    log.debug("Created Statement");
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    validateStatementWillReturnAResultSet(sql);
    return this.execute(sql, (Map<String, String>) null);
  }

  public ResultSet execute(String sql, Map<String, String> statementParams) throws SQLException {
    synchronized (this) {
      this.validateStatementIsNotClosed();
    }
    this.runningStatementId = UUID.randomUUID().toString();
    InputStream inputStream = null;
    try {
      log.info("Executing the statement with id {} : {}", this.runningStatementId, sql);
      if (resultSet != null && !this.resultSet.isClosed()) {
        this.resultSet.close();
        this.resultSet = null;
        log.info(
            "There was already an opened ResultSet for the statement object. The ResultSet is now closed.");
      }
      StatementInfoWrapper statementInfo =
          StatementUtil.extractStatementInfo(sql, runningStatementId);
      if (statementInfo.getType() == StatementInfoWrapper.StatementType.PARAM_SETTING) {
        this.connection.addProperty(statementInfo.getParam());
        log.debug("The property from the query {} was stored", this.runningStatementId);
      } else {
        Map<String, String> params =
            statementParams != null ? statementParams : this.getStatementParameters();
        inputStream = statementService.execute(statementInfo, this.sessionProperties, params);
        if (statementInfo.getType() == StatementInfoWrapper.StatementType.QUERY) {
          currentUpdateCount = -1; // Always -1 when returning a ResultSet
          Pair<Optional<String>, Optional<String>> dbNameAndTableNamePair =
              StatementUtil.extractDbNameAndTableNamePairFromQuery(statementInfo.getSql());

          resultSet =
              new FireboltResultSet(
                  inputStream,
                  dbNameAndTableNamePair.getRight().orElse("unknown"),
                  dbNameAndTableNamePair.getRight().orElse(this.sessionProperties.getDatabase()),
                  this.sessionProperties.getBufferSize(),
                  this.sessionProperties.isCompress(),
                  this,
                  this.sessionProperties.isLogResultSet());
        } else {
          currentUpdateCount = 0;
          CloseableUtils.close(inputStream);
        }
        log.info("The query with the id {} was executed with success", this.runningStatementId);
      }
    } catch (Exception ex) {
      CloseableUtils.close(inputStream);
      log.error(
          "An error happened while executing the statement with the id {}",
          this.runningStatementId,
          ex);
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
    try (ResultSet rs = this.execute(String.format(KILL_QUERY_SQL, statementId), statementParams)) {
    }
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException();
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
    if (resultSet != null && !resultSet.isClosed()) {
      resultSet.close();
      resultSet = null;
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
    return this.resultSet;
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    if (resultSet != null && !resultSet.isClosed()) {
      resultSet.close();
      resultSet = null;
    }
    currentUpdateCount = -1;
    return false;
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
    return this.execute(sql, (Map<String, String>) null) != null;
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
    if (!StatementUtil.isQuery(sql)) {
      throw new FireboltException("Cannot proceed: the statement does not return a ResultSet");
    }
  }
}
