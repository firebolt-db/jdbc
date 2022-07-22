package com.firebolt.jdbc.client.query;

import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.statement.StatementInfoWrapper;

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
