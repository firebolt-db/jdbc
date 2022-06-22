package io.firebolt.jdbc.statement;

import io.firebolt.QueryUtil;
import io.firebolt.jdbc.connection.FireboltConnectionTokens;
import io.firebolt.jdbc.connection.settings.FireboltProperties;
import io.firebolt.jdbc.exception.FireboltException;
import io.firebolt.jdbc.resultset.FireboltResultSet;
import io.firebolt.jdbc.service.FireboltQueryService;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Optional;
import java.util.UUID;

@Slf4j
public class FireboltStatementImpl extends AbstractStatement {

  private final FireboltQueryService fireboltQueryService;
  private final FireboltProperties sessionProperties;
  private final FireboltConnectionTokens connectionTokens;

  private boolean closeOnCompletion;
  int currentUpdateCount;

  int maxRows;
  boolean isClosed;

  private ResultSet resultSet;

  @Builder
  public FireboltStatementImpl(
      FireboltQueryService fireboltQueryService,
      FireboltProperties sessionProperties,
      FireboltConnectionTokens connectionTokens) {
    this.fireboltQueryService = fireboltQueryService;
    this.sessionProperties = sessionProperties;
    this.connectionTokens = connectionTokens;
    this.closeOnCompletion = true;
    this.currentUpdateCount = -1;
    this.isClosed = false;
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    Optional<Pair<String, String>> additionalProperties =
        QueryUtil.extractAdditionalProperties(sql);
    if (additionalProperties.isPresent()) {
      this.sessionProperties.addProperty(additionalProperties.get());
      return null;
    } else {
      String queryId = UUID.randomUUID().toString();
      InputStream inputStream =
          fireboltQueryService.executeQuery(
              sql, queryId, connectionTokens.getAccessToken(), sessionProperties);

      if (QueryUtil.isSelect(sql)) {
        currentUpdateCount = -1; // Always -1 when the result is a return a ResultSet
        resultSet =
            new FireboltResultSet(
                inputStream,
                QueryUtil.extractTableNameFromSelect(sql).orElse("unknown"),
                QueryUtil.extractDBNameFromSelect(sql).orElse(sessionProperties.getDatabase()),
                sessionProperties.getBufferSize());
      } else {
        currentUpdateCount = 0;
        try {
          inputStream.close();
          return null;
        } catch (Exception e) {
          throw new FireboltException("Error closing inputstream during update with query " + sql);
        }
      }
      return resultSet;
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
    if (resultSet != null) {
      resultSet.close();
      resultSet = null;
    }
    isClosed = true;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return isClosed;
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    return this.resultSet;
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    this.close();
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
