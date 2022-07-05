package io.firebolt.jdbc.statement;

import io.firebolt.jdbc.connection.FireboltConnection;
import io.firebolt.jdbc.connection.settings.FireboltProperties;
import io.firebolt.jdbc.exception.FireboltException;
import io.firebolt.jdbc.resultset.FireboltResultSet;
import io.firebolt.jdbc.service.FireboltStatementService;
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
  private String statementId;

  private boolean closeOnCompletion;
  int currentUpdateCount;

  int maxRows;
  boolean isClosed;

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
    this.closeOnCompletion = true;
    this.currentUpdateCount = -1;
    this.isClosed = false;
    this.connection = connection;
    log.debug("Created Statement");
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    return this.execute(sql, (Map<String, String>) null);
  }

  public ResultSet execute(String sql, Map<String, String> statementParams)
      throws SQLException {
    this.statementId = UUID.randomUUID().toString();
    try {
      log.debug("Executing query with id {} : {}", this.statementId, sql);
      if (resultSet != null && !this.resultSet.isClosed()) {
        this.resultSet.close();
        this.resultSet = null;
        log.info(
            "There was already an opened ResultSet for the statement object. The ResultSet is now closed.");
      }
      Optional<Pair<String, String>> additionalProperties = StatementUtil.extractPropertyFromQuery(sql);
      if (additionalProperties.isPresent()) {
        this.connection.addProperty(additionalProperties.get());
        log.debug("The property from the query {} was stored", this.statementId);
      } else {
        boolean isQuery = StatementUtil.isQuery(sql);
        StatementInfoWrapper statementInfoWrapper =
            StatementInfoWrapper.builder().sql(sql).id(statementId).query(isQuery).build();
        Map<String, String> params =
            statementParams != null ? statementParams : this.getStatementParameters();
        InputStream inputStream =
            statementService.execute(statementInfoWrapper, this.sessionProperties, params);
        if (isQuery) {
          currentUpdateCount = -1; // Always -1 when returning a ResultSet
          Pair<Optional<String>, Optional<String>> dbNameAndTableNamePair =
              StatementUtil.extractDbNameAndTableNamePairFromQuery(sql);

          resultSet =
              new FireboltResultSet(
                  inputStream,
                  dbNameAndTableNamePair.getRight().orElse("unknown"),
                  dbNameAndTableNamePair.getRight().orElse(this.sessionProperties.getDatabase()),
                  this.sessionProperties.getBufferSize(),
                  this.sessionProperties.isCompress());
        } else {
          currentUpdateCount = 0;
          closeStream(sql, inputStream);
        }
        log.debug("Query with id {} was executed with Success", this.statementId);
      }
    } catch (Exception ex) {
      log.error("Could not execute query with id {}", this.statementId, ex);
      throw ex;
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

  private void closeStream(String sql, InputStream inputStream) throws FireboltException {
    try {
      inputStream.close();
    } catch (Exception e) {
      throw new FireboltException(
          String.format("Error closing InputStream with query: %s", sql), e);
    }
  }

  @Override
  public void cancel() throws SQLException {
    try {
      log.debug("Cancelling query with id {}", statementId);
      if (this.statementId == null || isClosed()) {
        log.warn("Cannot cancel query as there is no query running");
      } else if ("localhost".equals(this.sessionProperties.getHost())
          || StringUtils.isEmpty(this.sessionProperties.getDatabase())
          || this.sessionProperties.isAggressiveCancelEnabled()) {
        cancelBySqlQuery();
      } else {
        statementService.cancel(this.statementId, this.sessionProperties);
      }
      log.debug("Query with id {} was cancelled", statementId);
    } catch (Exception e) {
      throw new FireboltException("Could not cancel query", e);
    }
  }

  private void cancelBySqlQuery() throws SQLException {
    log.debug("cancelling query");
    Map<String, String> statementParams = new HashMap<>(this.getStatementParameters());
    statementParams.put("use_standard_sql", "0");
    try (ResultSet rs =
        this.execute(String.format(KILL_QUERY_SQL, statementId), statementParams)) {}
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    // Ignore
  }

  @Override
  public Connection getConnection() throws SQLException {
    log.debug("Getting connection from statement");
    return this.connection;
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    log.debug("Getting more results: false");
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

  public synchronized void close(boolean removeFromConnection) throws SQLException {
    if (!this.isClosed) {
      this.isClosed = true;
      if (resultSet != null && !resultSet.isClosed()) {
        resultSet.close();
        resultSet = null;
      }
      log.debug("Statement closed");
      if (removeFromConnection) {
        connection.removeClosedStatement(this);
      }
    }
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
    this.executeQuery(sql);
    return StatementUtil.isQuery(sql);
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    return queryTimeout;
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    queryTimeout = seconds;
  }
}
