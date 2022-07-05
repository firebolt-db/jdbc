package io.firebolt.jdbc.client.query;

import io.firebolt.jdbc.statement.StatementInfoWrapper;
import io.firebolt.jdbc.connection.settings.FireboltProperties;
import io.firebolt.jdbc.exception.FireboltException;

import java.io.InputStream;
import java.util.Map;

public interface StatementClient {

  InputStream postSqlStatement(
      StatementInfoWrapper statementInfoWrapper,
      FireboltProperties connectionProperties,
      Map<String, String> statementParams)
      throws FireboltException;

  void postCancelSqlStatement(String id, FireboltProperties fireboltProperties, Map<String, String> statementParams)
      throws FireboltException;
}
