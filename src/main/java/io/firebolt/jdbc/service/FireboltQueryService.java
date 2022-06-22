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

  public InputStream executeQuery (
      String sql, String queryId, String accessToken, FireboltProperties properties) throws FireboltException {
    log.debug("Executing SQL: {}", sql);
    return queryClient.postSqlQuery(sql, queryId, accessToken, properties);
  }


}
