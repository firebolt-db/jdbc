package com.firebolt.jdbc.metadata;

import java.sql.ResultSet;

import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.resultset.FireboltResultSet;

/**
 * This class represents the database metadata for a system engine
 */
public class FireboltSystemEngineDatabaseMetadata extends FireboltDatabaseMetadata {

	public FireboltSystemEngineDatabaseMetadata(String url, FireboltConnection connection) {
		super(url, connection);
	}

	@Override
	public ResultSet getSchemas(String catalog, String schemaPattern) {
		return FireboltResultSet.empty();
	}

	@Override
	public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern,
			String columnNamePattern) {
		return FireboltResultSet.empty();
	}

	@Override
	public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] typesArr) {
		return FireboltResultSet.empty();
	}
}
