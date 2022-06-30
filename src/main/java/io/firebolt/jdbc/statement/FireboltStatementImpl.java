package io.firebolt.jdbc.statement;

import io.firebolt.QueryUtil;
import io.firebolt.jdbc.connection.FireboltConnection;
import io.firebolt.jdbc.connection.settings.FireboltProperties;
import io.firebolt.jdbc.exception.FireboltException;
import io.firebolt.jdbc.resultset.FireboltResultSet;
import io.firebolt.jdbc.service.FireboltQueryService;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Optional;
import java.util.UUID;

@Slf4j
public class FireboltStatementImpl extends AbstractStatement {

  private static final String KILL_QUERY_SQL =
      "KILL QUERY ON CLUSTER sql_cluster WHERE initial_query_id='%s'";
  private final FireboltQueryService fireboltQueryService;
  private FireboltProperties sessionProperties;
  private String queryId;

  private boolean closeOnCompletion;
  int currentUpdateCount;

  int maxRows;
  boolean isClosed;

  private ResultSet resultSet;

  private FireboltConnection connection;

  @Builder
  public FireboltStatementImpl(
      FireboltQueryService fireboltQueryService,
      FireboltProperties sessionProperties,
      FireboltConnection connection) {
    this.fireboltQueryService = fireboltQueryService;
    this.sessionProperties = sessionProperties;
    this.closeOnCompletion = true;
    this.currentUpdateCount = -1;
    this.isClosed = false;
    this.connection = connection;
    log.debug("Created Statement");
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    return this.executeQuery(sql, this.sessionProperties);
  }

  public ResultSet executeQuery(String sql, FireboltProperties properties) throws SQLException {
    this.queryId = UUID.randomUUID().toString();
    try {
      log.debug("Executing query with id {} : {}", this.queryId, sql);
      if (resultSet != null && !this.resultSet.isClosed()) {
        this.resultSet.close();
        this.resultSet = null;
        log.info(
            "There was already an opened ResultSet for the statement object. The ResultSet is now closed.");
      }
      Optional<Pair<String, String>> additionalProperties =
          QueryUtil.extractAdditionalProperties(sql);
      if (additionalProperties.isPresent()) {
        this.connection.addProperty(additionalProperties.get());
        log.debug("The property from the query {} was stored", this.queryId);
      } else {
        boolean isSelect = QueryUtil.isSelect(sql);
        log.debug("Query with id {} is a SELECT: {}", this.queryId, isSelect);
        InputStream inputStream =
            fireboltQueryService.executeQuery(sql, isSelect, queryId, properties);
        if (isSelect) {
          currentUpdateCount = -1; // Always -1 when returning a ResultSet
          Pair<Optional<String>, Optional<String>> dbNameAndTableNamePair =
              QueryUtil.extractDbNameAndTableNamePairFromQuery(sql);

          resultSet =
              new FireboltResultSet(
                  inputStream,
                  dbNameAndTableNamePair.getRight().orElse("unknown"),
                  dbNameAndTableNamePair
                      .getRight()
                      .orElse(properties.getDatabase()),
                  properties.getBufferSize(),
                  properties.isCompress());
        } else {
          currentUpdateCount = 0;
          closeStream(sql, inputStream);
        }
        log.debug("Query with id {} was executed with Success", this.queryId);
      }
    } catch (Exception ex) {
      log.error("Could not execute query with id {}", this.queryId, ex);
      throw ex;
    }
    return resultSet;
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
    log.debug("Cancelling query with id {}", queryId);
    if (this.queryId == null || isClosed()) {
      log.warn("Cannot cancel query as there is no query running");
    } else if ("localhost".equals(this.sessionProperties.getHost())
        || StringUtils.isEmpty(this.sessionProperties.getDatabase())
        || this.sessionProperties.isAggressiveCancelEnabled()) {
      cancelBySqlQuery();
    } else {
      fireboltQueryService.cancelQuery(this.queryId, this.sessionProperties);
    }
    log.debug("Query with id {} was cancelled", queryId);
  }

  private void cancelBySqlQuery() throws SQLException {
    log.debug("cancelling query");
    FireboltProperties temporaryProperties = FireboltProperties.copy(this.sessionProperties);
    temporaryProperties.addProperty("use_standard_sql", "0");
    try (ResultSet rs =
        this.executeQuery(String.format(KILL_QUERY_SQL, queryId), temporaryProperties)) {}
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
    return QueryUtil.isSelect(sql);
  }
}
