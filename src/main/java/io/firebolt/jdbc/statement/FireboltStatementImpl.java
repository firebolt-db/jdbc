package io.firebolt.jdbc.statement;

import io.firebolt.jdbc.connection.FireboltConnectionImpl;
import io.firebolt.jdbc.connection.FireboltConnectionTokens;
import io.firebolt.jdbc.connection.settings.FireboltProperties;
import io.firebolt.jdbc.resultset.FireboltResultSet;
import io.firebolt.jdbc.service.FireboltQueryService;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;

@Slf4j
@Builder
@RequiredArgsConstructor
public class FireboltStatementImpl extends AbstractStatement {
  
  private final FireboltQueryService fireboltQueryService;
  private final FireboltProperties sessionProperties;
  private final FireboltConnectionImpl fireboltConnectionImpl;
  private final FireboltConnectionTokens connectionTokens;

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    Optional<Pair<String, String>> additionalProperties =
        fireboltQueryService.extractAdditionalProperties(sql);

    if (additionalProperties.isPresent()) {
      this.sessionProperties.addProperty(additionalProperties.get());
    } else {
      String queryId = UUID.randomUUID().toString();
      InputStream inputStream =
          fireboltQueryService.executeQuery(
              sql, queryId, connectionTokens.getAccessToken(), sessionProperties);
      return new FireboltResultSet(
          inputStream,
          fireboltQueryService.extractTableName(sql),
          fireboltQueryService.extractDBName(sql).orElse(sessionProperties.getDatabase()),
          sessionProperties.getBufferSize());
    }
    return null;
  }
}
