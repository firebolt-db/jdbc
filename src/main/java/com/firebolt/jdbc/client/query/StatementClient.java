package com.firebolt.jdbc.client.query;

import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.statement.StatementInfoWrapper;
import java.io.InputStream;
import java.sql.SQLException;

public interface StatementClient {

	/**
	 * Post SQL statement
	 */
	InputStream executeSqlStatement(StatementInfoWrapper statementInfoWrapper, FireboltProperties connectionProperties,
									boolean systemEngine, int queryTimeout, boolean isServerAsync) throws SQLException;

	/**
	 * Call endpoint to abort a running SQL statement
	 */
	void abortStatement(String label, FireboltProperties fireboltProperties) throws SQLException;

	boolean isStatementRunning(String statementLabel);
}
