package io.firebolt.jdbc.statement;

import io.firebolt.QueryUtil;
import io.firebolt.jdbc.connection.FireboltConnectionTokens;
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
  private final FireboltConnectionTokens connectionTokens;

  private String queryId;

  private boolean closeOnCompletion;
  int currentUpdateCount;

  int maxRows;
  boolean isClosed;

  private ResultSet resultSet;

  private Connection connection;

  @Builder
  public FireboltStatementImpl(
      FireboltQueryService fireboltQueryService,
      FireboltProperties sessionProperties,
      FireboltConnectionTokens connectionTokens,
      Connection connection) {
    this.fireboltQueryService = fireboltQueryService;
    this.sessionProperties = sessionProperties;
    this.connectionTokens = connectionTokens;
    this.closeOnCompletion = true;
    this.currentUpdateCount = -1;
    this.isClosed = false;
    this.connection = connection;
    log.debug("Created statement!");
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    log.info("The executed query is: {}", sql);
    if(resultSet != null) {
      log.debug("There is already a rs for the statement !");
    }
    Optional<Pair<String, String>> additionalProperties =
        QueryUtil.extractAdditionalProperties(sql);
    if (additionalProperties.isPresent()) {
      this.sessionProperties.addProperty(additionalProperties.get());
      return null;
    } else {
      this.queryId = UUID.randomUUID().toString();
      InputStream inputStream =
          fireboltQueryService.executeQuery(
              sql, queryId, connectionTokens.getAccessToken(), sessionProperties);

      if (QueryUtil.isSelect(sql)) {
        currentUpdateCount = -1; // Always -1 when returning a ResultSet
        resultSet =
            new FireboltResultSet(
                inputStream,
                QueryUtil.extractTableNameFromSelect(sql).orElse("unknown"),
                QueryUtil.extractDBNameFromSelect(sql).orElse(sessionProperties.getDatabase() != null ? sessionProperties.getDatabase() : "unknown"),
                sessionProperties.getBufferSize());
      } else {
        currentUpdateCount = 0;
        try {
          inputStream.close();
          return null;
        } catch (Exception e) {
          throw new FireboltException(
              String.format("Error closing InputStream with query: %s", sql), e);
        }
      }
      return resultSet;
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
    FireboltProperties cachedProperties = FireboltProperties.copy(this.sessionProperties);
    this.sessionProperties.addProperty("use_standard_sql", "1");
    try (ResultSet rs = this.executeQuery(String.format(KILL_QUERY_SQL, queryId))) {
    } finally {
      this.sessionProperties = cachedProperties;
    }
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
    log.debug("Closing statement");
    if (resultSet != null && !resultSet.isClosed()) {
      resultSet.close();
      resultSet = null;
    }
    this.isClosed = true;
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
    this.executeQuery(sql);
    return QueryUtil.isSelect(sql);
  }
}
