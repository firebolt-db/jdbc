package io.firebolt.jdbc.client.query;

import io.firebolt.jdbc.connection.settings.FireboltProperties;
import io.firebolt.jdbc.exception.FireboltException;
import io.firebolt.jdbc.statement.StatementInfoWrapper;

import java.io.InputStream;
import java.util.Map;

public interface StatementClient {

  /**
   * Post SQL statement
   */
  InputStream postSqlStatement(
      StatementInfoWrapper statementInfoWrapper,
      FireboltProperties connectionProperties,
      Map<String, String> statementParams)
      throws FireboltException;

  /**
   * Call endpoint to abort a running SQL statement
   */
  void abortStatement(
      String id, FireboltProperties fireboltProperties, Map<String, String> statementParams)
      throws FireboltException;

  /**
   * Abort running HTTP request of a statement
   */
  void abortRunningHttpRequest(String id) throws FireboltException;
}
