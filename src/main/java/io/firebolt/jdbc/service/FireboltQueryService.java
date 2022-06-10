package io.firebolt.jdbc.service;

import io.firebolt.jdbc.client.query.QueryClient;
import io.firebolt.jdbc.connection.settings.FireboltProperties;
import io.firebolt.jdbc.exception.FireboltException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;

@RequiredArgsConstructor
@Slf4j
public class FireboltQueryService {

  private final QueryClient queryClient;

  public InputStream executeQuery(
      String sql, boolean isSelect, String queryId, String accessToken, FireboltProperties properties)
      throws FireboltException {
    return queryClient.postSqlQuery(sql, isSelect, queryId, accessToken, properties);
  }

  public void cancelQuery(String queryId, FireboltProperties properties) throws FireboltException {
    log.debug("Cancelling query with id: {}", queryId);
    queryClient.postCancelSqlQuery(queryId, properties);
  }
}
