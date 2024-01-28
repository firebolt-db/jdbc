package com.firebolt.jdbc.metadata;

import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.resultset.FireboltResultSet;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * This class represents the database metadata for a system engine
 */
public class FireboltSystemEngineDatabaseMetadata extends FireboltDatabaseMetadata {

	public FireboltSystemEngineDatabaseMetadata(String url, FireboltConnection connection) {
		super(url, connection);
	}

	@Override
	public ResultSet getSchemas() {
		return FireboltResultSet.empty();
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

	@Override
	public int getDatabaseMajorVersion() throws SQLException {
		return 0;
	}

	@Override
	public int getDatabaseMinorVersion() throws SQLException {
		return 0;
	}

	@Override
	public String getDatabaseProductVersion() throws SQLException {
		return "";
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		return true;
	}
}
