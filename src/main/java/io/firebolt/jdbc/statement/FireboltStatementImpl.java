package io.firebolt.jdbc.statement;

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
import java.util.Optional;
import java.util.UUID;

@Slf4j
@Builder
public class FireboltStatementImpl extends AbstractStatement {
  
  private final FireboltQueryService fireboltQueryService;
  private final FireboltProperties sessionProperties;
  private final FireboltConnectionTokens connectionTokens;

  @Builder.Default private boolean closeOnCompletion = true;
  @Builder.Default int currentUpdateCount = -1;

  int maxRows;
  @Builder.Default boolean isClosed = false;

  private ResultSet resultSet;

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    Optional<Pair<String, String>> additionalProperties =
        fireboltQueryService.extractAdditionalProperties(sql);
    if (additionalProperties.isPresent()) {
      this.sessionProperties.addProperty(additionalProperties.get());
      return null;
    } else {
      String queryId = UUID.randomUUID().toString();
      InputStream inputStream =
          fireboltQueryService.executeQuery(
              sql, queryId, connectionTokens.getAccessToken(), sessionProperties);
      resultSet =
          new FireboltResultSet(
              inputStream,
              fireboltQueryService.extractTableName(sql),
              fireboltQueryService.extractDBName(sql).orElse(sessionProperties.getDatabase()),
              sessionProperties.getBufferSize());
      return resultSet;
    }
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
}
