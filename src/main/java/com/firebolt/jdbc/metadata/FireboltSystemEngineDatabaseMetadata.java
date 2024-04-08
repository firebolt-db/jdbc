package com.firebolt.jdbc.metadata;

import com.firebolt.jdbc.connection.FireboltConnection;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * This class represents the database metadata for a system engine
 */
public class FireboltSystemEngineDatabaseMetadata extends FireboltDatabaseMetadata {
	private static final String INFORMATION_SCHEMA = "information_schema"; // the only schema available for system engine

	public FireboltSystemEngineDatabaseMetadata(String url, FireboltConnection connection) {
		super(url, connection);
	}

	@Override
	public ResultSet getSchemas() throws SQLException {
		return getSchemas(null, INFORMATION_SCHEMA);
	}

	@Override
	public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
		return super.getSchemas(catalog, fixSchema(schemaPattern));
	}

	@Override
	public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
		return super.getColumns(catalog, fixSchema(schemaPattern), tableNamePattern, columnNamePattern);
	}

	@Override
	public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] typesArr) throws SQLException {
		return super.getTables(catalog, fixSchema(schemaPattern),  tableNamePattern, typesArr);
	}

	/**
	 * Query {@code select * from information_schema.tables} returns all tables regardless the used engine.
	 * However, system engine cannot perform query against custom tables. So, according to our understanding
	 * custom tables should not be returned here because they cannot be considered "available" according to the
	 * documentation {@link java.sql.DatabaseMetaData#getTables(String, String, String, String[])}.
	 * <br/>
	 * So, this function "fixes" the given {@code schemaPattern}. If it is {@code null} or matches {@code information_schema}
	 * it remains unchanged, otherwise special not existing value is returned that causes functions that accept {@code schemaPattern}
	 * to return empty result set.
	 * <br/>
	 * This functionality will be probably changed in future if we allow system engine to perform queries from custom tables.
	 *
	 * @param schemaPattern the given schema pattern
	 * @return "fixed" schema pattern
	 */
	private String fixSchema(String schemaPattern) {
		return schemaPattern == null || INFORMATION_SCHEMA.contains(schemaPattern.replace("%", "").toLowerCase()) ? INFORMATION_SCHEMA : "does_not_exist";
	}
}
