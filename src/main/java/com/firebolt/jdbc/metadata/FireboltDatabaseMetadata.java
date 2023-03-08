package com.firebolt.jdbc.metadata;

import static com.firebolt.jdbc.metadata.MetadataColumns.*;
import static com.firebolt.jdbc.type.FireboltDataType.*;
import static java.sql.Types.VARCHAR;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;

import com.firebolt.jdbc.QueryResult;
import com.firebolt.jdbc.VersionUtil;
import com.firebolt.jdbc.annotation.ExcludeFromJacocoGeneratedReport;
import com.firebolt.jdbc.annotation.NotImplemented;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.resultset.FireboltResultSet;
import com.firebolt.jdbc.resultset.column.Column;
import com.firebolt.jdbc.type.FireboltDataType;

import lombok.CustomLog;

@CustomLog
public class FireboltDatabaseMetadata implements DatabaseMetaData {

	private static final String PUBLIC_SCHEMA_NAME = "public";
	private static final String INFORMATION_SCHEMA_NAME = "information_schema";
	private static final String CATALOG_SCHEMA_NAME = "catalog";
	private static final String QUOTE = "'";
	private final String url;
	private final FireboltConnection connection;
	private String databaseVersion;

	public FireboltDatabaseMetadata(String url, FireboltConnection connection) {
		this.url = url;
		this.connection = connection;
	}

	@Override
	public ResultSet getSchemas() throws SQLException {
		return getSchemas(null, null);
	}

	@Override
	public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
		String dbName = connection.getSessionProperties().getDatabase();
		List<String> publicRow = Arrays.asList(PUBLIC_SCHEMA_NAME, dbName);
		List<String> informationSchemaRow = Arrays.asList(INFORMATION_SCHEMA_NAME, dbName);
		List<String> catalogRow = Arrays.asList(CATALOG_SCHEMA_NAME, dbName);
		return FireboltResultSet.of(QueryResult.builder()
				.columns(Arrays.asList(QueryResult.Column.builder().name(TABLE_SCHEM).type(TEXT).build(),
						QueryResult.Column.builder().name(TABLE_CATALOG).type(TEXT).build()))
				.rows(Arrays.asList(publicRow, informationSchemaRow, catalogRow)).build());
	}

	@Override
	public ResultSet getTableTypes() throws SQLException {
		return FireboltResultSet.of(QueryResult.builder()
				.columns(Collections.singletonList(QueryResult.Column.builder().name(TABLE_TYPE).type(TEXT).build()))
				.rows(Arrays.asList(Arrays.asList("TABLE"), Arrays.asList("VIEW"))).build());
	}

	@Override
	public ResultSet getCatalogs() throws SQLException {
		return FireboltResultSet.of(QueryResult.builder()
				.columns(Collections.singletonList(QueryResult.Column.builder().name(TABLE_CAT).type(TEXT).build()))
				.rows(Collections.singletonList(Collections.singletonList(connection.getCatalog()))).build());
	}

	@Override
	public Connection getConnection() throws SQLException {
		return connection;
	}

	@Override
	public String getDatabaseProductName() throws SQLException {
		return "Firebolt";
	}

	@Override
	public String getURL() throws SQLException {
		return url;
	}

	@Override
	public String getDriverName() throws SQLException {
		return "Firebolt JDBC Driver";
	}

	@Override
	public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
		return level == Connection.TRANSACTION_NONE;
	}

	@Override
	public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
			throws SQLException {
		List<QueryResult.Column> columns = Arrays.asList(
				QueryResult.Column.builder().name(TABLE_CAT).type(TEXT).build(),
				QueryResult.Column.builder().name(TABLE_SCHEM).type(TEXT).build(),
				QueryResult.Column.builder().name(TABLE_NAME).type(TEXT).build(),
				QueryResult.Column.builder().name(COLUMN_NAME).type(TEXT).build(),
				QueryResult.Column.builder().name(DATA_TYPE).type(INTEGER).build(),
				QueryResult.Column.builder().name(TYPE_NAME).type(TEXT).build(),
				QueryResult.Column.builder().name(COLUMN_SIZE).type(INTEGER).build(),
				QueryResult.Column.builder().name(BUFFER_LENGTH).type(INTEGER).build(),
				QueryResult.Column.builder().name(DECIMAL_DIGITS).type(INTEGER).build(),
				QueryResult.Column.builder().name(NUM_PREC_RADIX).type(INTEGER).build(),
				QueryResult.Column.builder().name(NULLABLE).type(INTEGER).build(),
				QueryResult.Column.builder().name(REMARKS).type(TEXT).build(),
				QueryResult.Column.builder().name(COLUMN_DEF).type(TEXT).build(),
				QueryResult.Column.builder().name(SQL_DATA_TYPE).type(INTEGER).build(),
				QueryResult.Column.builder().name(SQL_DATETIME_SUB).type(INTEGER).build(),
				QueryResult.Column.builder().name(CHAR_OCTET_LENGTH).type(INTEGER).build(),
				QueryResult.Column.builder().name(ORDINAL_POSITION).type(INTEGER).build(),
				QueryResult.Column.builder().name(IS_NULLABLE).type(TEXT).build(),
				QueryResult.Column.builder().name(SCOPE_CATALOG).type(TEXT).build(),
				QueryResult.Column.builder().name(SCOPE_SCHEMA).type(TEXT).build(),
				QueryResult.Column.builder().name(SCOPE_TABLE).type(TEXT).build(),
				QueryResult.Column.builder().name(SOURCE_DATA_TYPE).type(INTEGER).build(),
				QueryResult.Column.builder().name(IS_AUTOINCREMENT).type(TEXT).build(),
				QueryResult.Column.builder().name(IS_GENERATEDCOLUMN).type(TEXT).build());

		List<List<?>> rows = new ArrayList<>();
		String query = MetadataUtil.getColumnsQuery(schemaPattern, tableNamePattern, columnNamePattern);
		try (Statement statement = this.createStatementWithRequiredPropertiesToQuerySystem();
				ResultSet columnDescription = statement.executeQuery(query)) {
			while (columnDescription.next()) {
				List<?> row;
				Column columnInfo = Column.of(columnDescription.getString("data_type"),
						columnDescription.getString("column_name"));
				row = Arrays.asList(connection.getCatalog(), // TABLE_CAT
						columnDescription.getString("table_schema"), // schema
						columnDescription.getString("table_name"), // table name
						columnDescription.getString("column_name"), // column name
						String.valueOf(columnInfo.getType().getDataType().getSqlType()), // sql data type
						columnInfo.getType().getCompactTypeName(), // shorter type name
						Optional.ofNullable(columnInfo.getType().getPrecision()).map(String::valueOf).orElse(null),// column size
						null, // buffer length (not used, see Javadoc)
						Optional.ofNullable(columnInfo.getType().getScale()).map(String::valueOf).orElse(null), // DECIMAL_DIGITS
						String.valueOf(COMMON_RADIX), // radix
						isColumnNullable(columnDescription) ? columnNullable : columnNoNulls
						, null, // description of the column
						StringUtils.isNotBlank(columnDescription.getString("column_default"))
								? columnDescription.getString("column_default")
								: null, // default value for the column: null,
						null, // SQL_DATA_TYPE - reserved for future use (see javadoc)
						null, // SQL_DATETIME_SUB - reserved for future use (see javadoc)
						null, // CHAR_OCTET_LENGTH - The maximum
								// length of binary and character
								// based columns (null for others)
						columnDescription.getInt("ordinal_position"), // The ordinal position starting from 1
						isColumnNullable(columnDescription) ? "YES" : "NO", null, // "SCOPE_CATALOG - Unused
						null, // "SCOPE_SCHEMA" - Unused
						null, // "SCOPE_TABLE" - Unused
						null, // "SOURCE_DATA_TYPE" - Unused
						"NO", // IS_AUTOINCREMENT - Not supported
						"NO"); // IS_GENERATEDCOLUMN - Not supported
				rows.add(row);
			}
			return FireboltResultSet.of(QueryResult.builder().rows(rows).columns(columns).build());
		}
	}

	private boolean isColumnNullable(ResultSet columnDescription) throws SQLException {
		try {
			return columnDescription.getInt("is_nullable") == 1;
		} catch (Exception e) {
			return columnDescription.getBoolean("is_nullable");
		}
	}

	@Override
	public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] typesArr)
			throws SQLException {
		List<List<?>> rows = Stream
				.of(this.getTables(catalog, schemaPattern, tableNamePattern, typesArr, false),
						this.getTables(catalog, schemaPattern, tableNamePattern, typesArr, true))
				.flatMap(Collection::stream).collect(Collectors.toList());

		return FireboltResultSet.of(QueryResult.builder()
				.columns(Arrays.asList(QueryResult.Column.builder().name(TABLE_CAT).type(TEXT).build(),
						QueryResult.Column.builder().name(TABLE_SCHEM).type(TEXT).build(),
						QueryResult.Column.builder().name(TABLE_NAME).type(TEXT).build(),
						QueryResult.Column.builder().name(TABLE_TYPE).type(TEXT).build(),
						QueryResult.Column.builder().name(REMARKS).type(TEXT).build(),
						QueryResult.Column.builder().name(TYPE_CAT).type(TEXT).build(),
						QueryResult.Column.builder().name(TYPE_SCHEM).type(TEXT).build(),
						QueryResult.Column.builder().name(TYPE_NAME).type(TEXT).build(),
						QueryResult.Column.builder().name(SELF_REFERENCING_COL_NAME).type(TEXT).build(),
						QueryResult.Column.builder().name(REF_GENERATION).type(TEXT).build()))
				.rows(rows).build());
	}

	private List<List<?>> getTables(String catalog, String schemaPattern, String tableNamePattern, String[] typesArr,
			boolean isView) throws SQLException {
		List<List<?>> rows = new ArrayList<>();

		String query = isView ? MetadataUtil.getViewsQuery(catalog, schemaPattern, tableNamePattern)
				: MetadataUtil.getTablesQuery(catalog, schemaPattern, tableNamePattern);
		try (Statement statement = this.createStatementWithRequiredPropertiesToQuerySystem();
				ResultSet tables = statement.executeQuery(query)) {

			Set<String> types = typesArr != null ? new HashSet<>(Arrays.asList(typesArr)) : null;
			while (tables.next()) {
				List<String> row = new ArrayList<>();
				row.add(connection.getCatalog());
				row.add(tables.getString("table_schema"));
				row.add(tables.getString("table_name"));
				String tableType = isView ? "VIEW" : "TABLE";
				row.add(tableType);
				for (int i = 3; i < 9; i++) {
					row.add(null);
				}
				if (types == null || types.contains(tableType)) {
					rows.add(row);
				}
			}
		}
		return rows;
	}

	@Override
	public ResultSet getTypeInfo() throws SQLException {
		List<QueryResult.Column> columns = Arrays.asList(
				QueryResult.Column.builder().name(TYPE_NAME).type(TEXT).build(),
				QueryResult.Column.builder().name(DATA_TYPE).type(INTEGER).build(),
				QueryResult.Column.builder().name(PRECISION).type(INTEGER).build(),
				QueryResult.Column.builder().name(LITERAL_PREFIX).type(TEXT).build(),
				QueryResult.Column.builder().name(LITERAL_SUFFIX).type(TEXT).build(),
				QueryResult.Column.builder().name(CREATE_PARAMS).type(TEXT).build(),
				QueryResult.Column.builder().name(NULLABLE).type(INTEGER).build(),
				QueryResult.Column.builder().name(CASE_SENSITIVE).type(BOOLEAN).build(),
				QueryResult.Column.builder().name(SEARCHABLE).type(INTEGER).build(),
				QueryResult.Column.builder().name(UNSIGNED_ATTRIBUTE).type(BOOLEAN).build(),
				QueryResult.Column.builder().name(FIXED_PREC_SCALE).type(BOOLEAN).build(),
				QueryResult.Column.builder().name(AUTO_INCREMENT).type(BOOLEAN).build(),
				QueryResult.Column.builder().name(LOCAL_TYPE_NAME).type(TEXT).build(),
				QueryResult.Column.builder().name(MINIMUM_SCALE).type(INTEGER).build(),
				QueryResult.Column.builder().name(MAXIMUM_SCALE).type(INTEGER).build(),
				QueryResult.Column.builder().name(SQL_DATA_TYPE).type(INTEGER).build(),
				QueryResult.Column.builder().name(SQL_DATETIME_SUB).type(INTEGER).build(),
				QueryResult.Column.builder().name(NUM_PREC_RADIX).type(INTEGER).build());

		List<List<?>> rows = new ArrayList<>();
		List<FireboltDataType> usableTypes = Arrays.asList(INTEGER, BIG_INT, REAL, DOUBLE_PRECISION, TEXT, DATE,
				TIMESTAMP, NUMERIC, ARRAY, TUPLE, BYTEA, BOOLEAN);
		usableTypes
				.forEach(
						type -> rows.add(Arrays.asList(type.getDisplayName(), type.getSqlType(),
								type.getPrecision(), QUOTE, // LITERAL_PREFIX
								QUOTE, // LITERAL_SUFFIX
								null, // Description of the creation parameters - can be null (can set if needed
										// in the future)
								typeNullableUnknown, // It depends - A type can be nullable or not depending on
								// the presence of the additional keyword Nullable()
								type.isCaseSensitive(), type.getSqlType() == VARCHAR ? typeSearchable
										: typePredBasic, /*
															 * SEARCHABLE - LIKE can only be used for VARCHAR
															 */
								!type.isSigned(),
								false, // FIXED_PREC_SCALE - indicates if the type can be a money value.
												// Always
												// false as we do not have a money type
								false, // AUTO_INCREMENT
								null, // LOCAL_TYPE_NAME
								type.getMinScale(), // MINIMUM_SCALE
								type.getMaxScale(), // MAXIMUM_SCALE
								null, // SQL_DATA_TYPE - Not needed - reserved for future use
								null, // SQL_DATETIME_SUB - Not needed - reserved for future use
								COMMON_RADIX)));

		return FireboltResultSet.of(QueryResult.builder().columns(columns).rows(rows).build());
	}

	private Statement createStatementWithRequiredPropertiesToQuerySystem() throws SQLException {
		FireboltConnection fireboltConnection = (FireboltConnection) this.getConnection();
		String useStandardSql = fireboltConnection.getSessionProperties().getAdditionalProperties()
				.get("use_standard_sql");
		if ("0".equals(useStandardSql)) {
			FireboltProperties properties = fireboltConnection.getSessionProperties();
			FireboltProperties tmpProperties = FireboltProperties.copy(properties);
			tmpProperties.addProperty("use_standard_sql", "1");
			return fireboltConnection.createStatement(tmpProperties);
		} else {
			return fireboltConnection.createStatement();
		}
	}

	@Override
	public int getDriverMajorVersion() {
		return VersionUtil.getMajorDriverVersion();
	}

	@Override
	public int getDriverMinorVersion() {
		return VersionUtil.getDriverMinorVersion();
	}

	@Override
	public String getDatabaseProductVersion() throws SQLException {
		if (this.databaseVersion == null) {
			String engine = this.connection.getEngine();
			try (Statement statement = createStatementWithRequiredPropertiesToQuerySystem()) {
				String query = MetadataUtil.getDatabaseVersionQuery(engine);
				ResultSet rs = statement.executeQuery(query);
				rs.next();
				this.databaseVersion = rs.getString(1);
			}
		}
		return this.databaseVersion;
	}

	@Override
	public int getDatabaseMajorVersion() throws SQLException {
		return VersionUtil.extractMajorVersion(getDatabaseProductVersion());
	}

	@Override
	public int getDatabaseMinorVersion() throws SQLException {
		return VersionUtil.extractMinorVersion(getDatabaseProductVersion());
	}

	@Override
	public int getJDBCMajorVersion() throws SQLException {
		return VersionUtil.getMajorDriverVersion();
	}

	@Override
	public int getJDBCMinorVersion() throws SQLException {
		return VersionUtil.getDriverMinorVersion();
	}

	@Override
	public String getDriverVersion() throws SQLException {
		return VersionUtil.getDriverVersion();
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return iface.isAssignableFrom(getClass());
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		if (iface.isAssignableFrom(getClass())) {
			return iface.cast(this);
		}
		throw new SQLException("Cannot unwrap to " + iface.getName());
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean allProceduresAreCallable() throws SQLException {
		return true;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean allTablesAreSelectable() throws SQLException {
		return true;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public String getUserName() throws SQLException {
		return connection.getSessionProperties().getUser();
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean isReadOnly() throws SQLException {
		return true;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean nullsAreSortedHigh() throws SQLException {
		return true;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean nullsAreSortedLow() throws SQLException {
		return !nullsAreSortedHigh();
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean nullsAreSortedAtStart() throws SQLException {
		return true;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean nullsAreSortedAtEnd() throws SQLException {
		return !nullsAreSortedAtStart();
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean usesLocalFiles() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean usesLocalFilePerTable() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsMixedCaseIdentifiers() throws SQLException {
		return true;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean storesUpperCaseIdentifiers() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean storesLowerCaseIdentifiers() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean storesMixedCaseIdentifiers() throws SQLException {
		return true;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
		return true;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
		return true;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public String getIdentifierQuoteString() throws SQLException {
		return "\"";
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public String getSQLKeywords() throws SQLException {
		// Firebolt reserved words minus SQL:2003 keywords
		return "BOOL,CONCAT,COPY,DATABASE,DATETIME,DOUBLECOLON,DOW,"
				+ "DOY,EMPTY_IDENTIFIER,EPOCH,EXPLAIN,EXTRACT,FIRST,GENERATE,ILIKE,ISNULL,"
				+ "JOIN_TYPE,LIMIT,LIMIT_DISTINCT,LONG,NEXT,OFFSET,PRIMARY,QUARTER,SAMPLE,SHOW,TEXT,"
				+ "TOP,TRIM,TRUNCATE,UNKNOWN_CHAR,UNTERMINATED_STRING,WEEK";
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public String getNumericFunctions() throws SQLException {
		return "";
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public String getStringFunctions() throws SQLException {
		return "";
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public String getSystemFunctions() throws SQLException {
		return "";
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public String getTimeDateFunctions() throws SQLException {
		return "";
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public String getSearchStringEscape() throws SQLException {
		return "\\";
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public String getExtraNameCharacters() throws SQLException {
		return "";
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsAlterTableWithAddColumn() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsAlterTableWithDropColumn() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsColumnAliasing() throws SQLException {
		return true;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean nullPlusNonNullIsNull() throws SQLException {
		return true;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsConvert() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsConvert(int fromType, int toType) throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsTableCorrelationNames() throws SQLException {
		return true;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsDifferentTableCorrelationNames() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsExpressionsInOrderBy() throws SQLException {
		return true;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsOrderByUnrelated() throws SQLException {
		return true;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsGroupBy() throws SQLException {
		return true;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsGroupByUnrelated() throws SQLException {
		return true;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsGroupByBeyondSelect() throws SQLException {
		return true;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsLikeEscapeClause() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsMultipleResultSets() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsMultipleTransactions() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsNonNullableColumns() throws SQLException {
		return true;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsMinimumSQLGrammar() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsCoreSQLGrammar() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsExtendedSQLGrammar() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsANSI92EntryLevelSQL() throws SQLException {
		// We do not support it (eg: we would need to be compliant with JDBC and support
		// 'schema')
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsANSI92IntermediateSQL() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsANSI92FullSQL() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsIntegrityEnhancementFacility() throws SQLException {
		// Similar approach as pgjdbc: we assume it means supported constraints
		return true;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsOuterJoins() throws SQLException {
		return true;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsFullOuterJoins() throws SQLException {
		return true;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsLimitedOuterJoins() throws SQLException {
		return true;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public String getSchemaTerm() throws SQLException {
		return "schema";
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public String getProcedureTerm() throws SQLException {
		return "procedure";
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public String getCatalogTerm() throws SQLException {
		return "database";
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean isCatalogAtStart() throws SQLException {
		// it is currently not supported but it will be soon
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public String getCatalogSeparator() throws SQLException {
		return ".";
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsSchemasInDataManipulation() throws SQLException {
		return true;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsSchemasInProcedureCalls() throws SQLException {
		return true;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsSchemasInTableDefinitions() throws SQLException {
		return true;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsSchemasInIndexDefinitions() throws SQLException {
		return true;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
		return true;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsCatalogsInDataManipulation() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsCatalogsInProcedureCalls() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsCatalogsInTableDefinitions() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsPositionedDelete() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsPositionedUpdate() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsSelectForUpdate() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsStoredProcedures() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsSubqueriesInComparisons() throws SQLException {
		return true;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsSubqueriesInExists() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsSubqueriesInIns() throws SQLException {
		return true;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsSubqueriesInQuantifieds() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsCorrelatedSubqueries() throws SQLException {
		return true;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsUnion() throws SQLException {
		return true;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsUnionAll() throws SQLException {
		return true;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public int getMaxBinaryLiteralLength() throws SQLException {
		return 0;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public int getMaxCharLiteralLength() throws SQLException {
		return 0;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public int getMaxColumnNameLength() throws SQLException {
		return 0;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public int getMaxColumnsInGroupBy() throws SQLException {
		return 0;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public int getMaxColumnsInIndex() throws SQLException {
		return 0;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public int getMaxColumnsInOrderBy() throws SQLException {
		return 0;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public int getMaxColumnsInSelect() throws SQLException {
		return 0;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public int getMaxColumnsInTable() throws SQLException {
		return 0;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public int getMaxConnections() throws SQLException {
		return 0;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public int getMaxCursorNameLength() throws SQLException {
		return 0;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public int getMaxIndexLength() throws SQLException {
		return 0;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public int getMaxSchemaNameLength() throws SQLException {
		return 0;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public int getMaxProcedureNameLength() throws SQLException {
		return 0;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public int getMaxCatalogNameLength() throws SQLException {
		return 0;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public int getMaxRowSize() throws SQLException {
		return 0;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public int getMaxStatementLength() throws SQLException {
		return 0;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public int getMaxStatements() throws SQLException {
		return 0;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public int getMaxTableNameLength() throws SQLException {
		return 0;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public int getMaxTablesInSelect() throws SQLException {
		return 0;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public int getMaxUserNameLength() throws SQLException {
		return 0;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public int getDefaultTransactionIsolation() throws SQLException {
		return Connection.TRANSACTION_NONE;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsTransactions() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsResultSetType(int type) throws SQLException {
		return ResultSet.TYPE_FORWARD_ONLY == type;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
		return false;
	}

	/*
	 * The methods related to the visibility of updated ResultSets do not apply
	 * since we do not support updating ResultSet objects
	 */
	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean ownUpdatesAreVisible(int type) throws SQLException {
		return true;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean ownDeletesAreVisible(int type) throws SQLException {
		return true;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean ownInsertsAreVisible(int type) throws SQLException {
		return true;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean othersUpdatesAreVisible(int type) throws SQLException {
		return true;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean othersDeletesAreVisible(int type) throws SQLException {
		return true;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean othersInsertsAreVisible(int type) throws SQLException {
		return true;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean updatesAreDetected(int type) throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean deletesAreDetected(int type) throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean insertsAreDetected(int type) throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsBatchUpdates() throws SQLException {
		// We support it partially (via FireboltPreparedStatement but not with the
		// 'basic' FireboltStatement )
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsSavepoints() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsNamedParameters() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsMultipleOpenResults() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsGetGeneratedKeys() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsResultSetHoldability(int holdability) throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public int getResultSetHoldability() throws SQLException {
		// N/A applicable as we do not support transactions
		return 0;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public int getSQLStateType() throws SQLException {
		return sqlStateSQL;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean locatorsUpdateCopy() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsStatementPooling() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public RowIdLifetime getRowIdLifetime() throws SQLException {
		return RowIdLifetime.ROWID_UNSUPPORTED;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
			String columnNamePattern) throws SQLException {
		return FireboltResultSet.empty();
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
			throws SQLException {
		return FireboltResultSet.empty();
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
		return FireboltResultSet.empty();
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
		return FireboltResultSet.empty();
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
			String attributeNamePattern) throws SQLException {
		return FireboltResultSet.empty();
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
			throws SQLException {
		return FireboltResultSet.empty();
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
			throws SQLException {
		return FireboltResultSet.empty();
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
			throws SQLException {
		return FireboltResultSet.empty();
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
			throws SQLException {
		return FireboltResultSet.empty();
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
		return FireboltResultSet.empty();
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
		return FireboltResultSet.empty();
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
		return FireboltResultSet.empty();
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
		return FireboltResultSet.empty();
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
			String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
		return FireboltResultSet.empty();
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
			throws SQLException {
		return FireboltResultSet.empty();
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public ResultSet getClientInfoProperties() throws SQLException {
		return FireboltResultSet.empty();
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
			throws SQLException {
		return FireboltResultSet.empty();
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
			String columnNamePattern) throws SQLException {
		return FireboltResultSet.empty();
	}
	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
			String columnNamePattern) throws SQLException {
		return FireboltResultSet.empty();
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean generatedKeyAlwaysReturned() throws SQLException {
		return false;
	}
}
