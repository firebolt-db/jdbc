package com.firebolt.jdbc.metadata;

import com.firebolt.jdbc.GenericWrapper;
import com.firebolt.jdbc.QueryResult;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.resultset.FireboltResultSet;
import com.firebolt.jdbc.resultset.column.Column;
import com.firebolt.jdbc.type.FireboltDataType;
import com.firebolt.jdbc.util.VersionUtil;
import lombok.CustomLog;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.firebolt.jdbc.metadata.MetadataColumns.ASC_OR_DESC;
import static com.firebolt.jdbc.metadata.MetadataColumns.ATTR_DEF;
import static com.firebolt.jdbc.metadata.MetadataColumns.ATTR_NAME;
import static com.firebolt.jdbc.metadata.MetadataColumns.ATTR_SIZE;
import static com.firebolt.jdbc.metadata.MetadataColumns.ATTR_TYPE_NAME;
import static com.firebolt.jdbc.metadata.MetadataColumns.AUTO_INCREMENT;
import static com.firebolt.jdbc.metadata.MetadataColumns.BASE_TYPE;
import static com.firebolt.jdbc.metadata.MetadataColumns.BUFFER_LENGTH;
import static com.firebolt.jdbc.metadata.MetadataColumns.CARDINALITY;
import static com.firebolt.jdbc.metadata.MetadataColumns.CASE_SENSITIVE;
import static com.firebolt.jdbc.metadata.MetadataColumns.CHAR_OCTET_LENGTH;
import static com.firebolt.jdbc.metadata.MetadataColumns.CLASS_NAME;
import static com.firebolt.jdbc.metadata.MetadataColumns.COLUMN_DEF;
import static com.firebolt.jdbc.metadata.MetadataColumns.COLUMN_NAME;
import static com.firebolt.jdbc.metadata.MetadataColumns.COLUMN_SIZE;
import static com.firebolt.jdbc.metadata.MetadataColumns.COLUMN_TYPE;
import static com.firebolt.jdbc.metadata.MetadataColumns.COLUMN_USAGE;
import static com.firebolt.jdbc.metadata.MetadataColumns.COMMON_RADIX;
import static com.firebolt.jdbc.metadata.MetadataColumns.CREATE_PARAMS;
import static com.firebolt.jdbc.metadata.MetadataColumns.DATA_TYPE;
import static com.firebolt.jdbc.metadata.MetadataColumns.DECIMAL_DIGITS;
import static com.firebolt.jdbc.metadata.MetadataColumns.DEFAULT_VALUE;
import static com.firebolt.jdbc.metadata.MetadataColumns.DEFERRABILITY;
import static com.firebolt.jdbc.metadata.MetadataColumns.DELETE_RULE;
import static com.firebolt.jdbc.metadata.MetadataColumns.DESCRIPTION;
import static com.firebolt.jdbc.metadata.MetadataColumns.FILTER_CONDITION;
import static com.firebolt.jdbc.metadata.MetadataColumns.FIXED_PREC_SCALE;
import static com.firebolt.jdbc.metadata.MetadataColumns.FKCOLUMN_NAME;
import static com.firebolt.jdbc.metadata.MetadataColumns.FKTABLE_CAT;
import static com.firebolt.jdbc.metadata.MetadataColumns.FKTABLE_NAME;
import static com.firebolt.jdbc.metadata.MetadataColumns.FKTABLE_SCHEM;
import static com.firebolt.jdbc.metadata.MetadataColumns.FK_NAME;
import static com.firebolt.jdbc.metadata.MetadataColumns.FUNCTION_CAT;
import static com.firebolt.jdbc.metadata.MetadataColumns.FUNCTION_NAME;
import static com.firebolt.jdbc.metadata.MetadataColumns.FUNCTION_SCHEM;
import static com.firebolt.jdbc.metadata.MetadataColumns.FUNCTION_TYPE;
import static com.firebolt.jdbc.metadata.MetadataColumns.GRANTEE;
import static com.firebolt.jdbc.metadata.MetadataColumns.GRANTOR;
import static com.firebolt.jdbc.metadata.MetadataColumns.INDEX_NAME;
import static com.firebolt.jdbc.metadata.MetadataColumns.INDEX_QUALIFIER;
import static com.firebolt.jdbc.metadata.MetadataColumns.IS_AUTOINCREMENT;
import static com.firebolt.jdbc.metadata.MetadataColumns.IS_GENERATEDCOLUMN;
import static com.firebolt.jdbc.metadata.MetadataColumns.IS_GRANTABLE;
import static com.firebolt.jdbc.metadata.MetadataColumns.IS_NULLABLE;
import static com.firebolt.jdbc.metadata.MetadataColumns.KEY_SEQ;
import static com.firebolt.jdbc.metadata.MetadataColumns.LENGTH;
import static com.firebolt.jdbc.metadata.MetadataColumns.LITERAL_PREFIX;
import static com.firebolt.jdbc.metadata.MetadataColumns.LITERAL_SUFFIX;
import static com.firebolt.jdbc.metadata.MetadataColumns.LOCAL_TYPE_NAME;
import static com.firebolt.jdbc.metadata.MetadataColumns.MAXIMUM_SCALE;
import static com.firebolt.jdbc.metadata.MetadataColumns.MAX_LEN;
import static com.firebolt.jdbc.metadata.MetadataColumns.MINIMUM_SCALE;
import static com.firebolt.jdbc.metadata.MetadataColumns.NAME;
import static com.firebolt.jdbc.metadata.MetadataColumns.NON_UNIQUE;
import static com.firebolt.jdbc.metadata.MetadataColumns.NULLABLE;
import static com.firebolt.jdbc.metadata.MetadataColumns.NUM_PREC_RADIX;
import static com.firebolt.jdbc.metadata.MetadataColumns.ORDINAL_POSITION;
import static com.firebolt.jdbc.metadata.MetadataColumns.PAGES;
import static com.firebolt.jdbc.metadata.MetadataColumns.PKCOLUMN_NAME;
import static com.firebolt.jdbc.metadata.MetadataColumns.PKTABLE_CAT;
import static com.firebolt.jdbc.metadata.MetadataColumns.PKTABLE_NAME;
import static com.firebolt.jdbc.metadata.MetadataColumns.PKTABLE_SCHEM;
import static com.firebolt.jdbc.metadata.MetadataColumns.PK_NAME;
import static com.firebolt.jdbc.metadata.MetadataColumns.PRECISION;
import static com.firebolt.jdbc.metadata.MetadataColumns.PRIVILEGE;
import static com.firebolt.jdbc.metadata.MetadataColumns.PROCEDURE_CAT;
import static com.firebolt.jdbc.metadata.MetadataColumns.PROCEDURE_NAME;
import static com.firebolt.jdbc.metadata.MetadataColumns.PROCEDURE_SCHEM;
import static com.firebolt.jdbc.metadata.MetadataColumns.PROCEDURE_TYPE;
import static com.firebolt.jdbc.metadata.MetadataColumns.PSEUDO_COLUMN;
import static com.firebolt.jdbc.metadata.MetadataColumns.RADIX;
import static com.firebolt.jdbc.metadata.MetadataColumns.REF_GENERATION;
import static com.firebolt.jdbc.metadata.MetadataColumns.REMARKS;
import static com.firebolt.jdbc.metadata.MetadataColumns.SCALE;
import static com.firebolt.jdbc.metadata.MetadataColumns.SCOPE;
import static com.firebolt.jdbc.metadata.MetadataColumns.SCOPE_CATALOG;
import static com.firebolt.jdbc.metadata.MetadataColumns.SCOPE_SCHEMA;
import static com.firebolt.jdbc.metadata.MetadataColumns.SCOPE_TABLE;
import static com.firebolt.jdbc.metadata.MetadataColumns.SEARCHABLE;
import static com.firebolt.jdbc.metadata.MetadataColumns.SELF_REFERENCING_COL_NAME;
import static com.firebolt.jdbc.metadata.MetadataColumns.SOURCE_DATA_TYPE;
import static com.firebolt.jdbc.metadata.MetadataColumns.SPECIFIC_NAME;
import static com.firebolt.jdbc.metadata.MetadataColumns.SQL_DATA_TYPE;
import static com.firebolt.jdbc.metadata.MetadataColumns.SQL_DATETIME_SUB;
import static com.firebolt.jdbc.metadata.MetadataColumns.SUPERTABLE_NAME;
import static com.firebolt.jdbc.metadata.MetadataColumns.SUPERTYPE_CAT;
import static com.firebolt.jdbc.metadata.MetadataColumns.SUPERTYPE_NAME;
import static com.firebolt.jdbc.metadata.MetadataColumns.SUPERTYPE_SCHEM;
import static com.firebolt.jdbc.metadata.MetadataColumns.TABLE_CAT;
import static com.firebolt.jdbc.metadata.MetadataColumns.TABLE_CATALOG;
import static com.firebolt.jdbc.metadata.MetadataColumns.TABLE_NAME;
import static com.firebolt.jdbc.metadata.MetadataColumns.TABLE_SCHEM;
import static com.firebolt.jdbc.metadata.MetadataColumns.TABLE_TYPE;
import static com.firebolt.jdbc.metadata.MetadataColumns.TYPE;
import static com.firebolt.jdbc.metadata.MetadataColumns.TYPE_CAT;
import static com.firebolt.jdbc.metadata.MetadataColumns.TYPE_NAME;
import static com.firebolt.jdbc.metadata.MetadataColumns.TYPE_SCHEM;
import static com.firebolt.jdbc.metadata.MetadataColumns.UNSIGNED_ATTRIBUTE;
import static com.firebolt.jdbc.metadata.MetadataColumns.UPDATE_RULE;
import static com.firebolt.jdbc.type.FireboltDataType.ARRAY;
import static com.firebolt.jdbc.type.FireboltDataType.BIG_INT;
import static com.firebolt.jdbc.type.FireboltDataType.BOOLEAN;
import static com.firebolt.jdbc.type.FireboltDataType.BYTEA;
import static com.firebolt.jdbc.type.FireboltDataType.DATE;
import static com.firebolt.jdbc.type.FireboltDataType.DOUBLE_PRECISION;
import static com.firebolt.jdbc.type.FireboltDataType.INTEGER;
import static com.firebolt.jdbc.type.FireboltDataType.NUMERIC;
import static com.firebolt.jdbc.type.FireboltDataType.REAL;
import static com.firebolt.jdbc.type.FireboltDataType.TEXT;
import static com.firebolt.jdbc.type.FireboltDataType.TIMESTAMP;
import static com.firebolt.jdbc.type.FireboltDataType.TUPLE;
import static java.lang.String.format;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static java.sql.Types.VARCHAR;
import static java.util.Map.entry;
import static java.util.regex.Pattern.CASE_INSENSITIVE;
import static java.util.regex.Pattern.compile;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

@CustomLog
@SuppressWarnings("java:S6204") // compatibility with JDK 11
public class FireboltDatabaseMetadata implements DatabaseMetaData, GenericWrapper {

	private static final String TABLE = "TABLE";
	private static final String VIEW = "VIEW";
	private static final String QUOTE = "'";
	private static final int MAX_IDENTIFIER_LENGTH = 63;
	private static final int MAX_LITERAL_LENGTH = 0x40000; // 262144

	private final String url;
	private final FireboltConnection connection;
	private volatile String databaseVersion;

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
		String catalogClause = catalog == null ? null : format("%s LIKE '%s'", TABLE_CATALOG, catalog);
		String schemaClause = schemaPattern == null ? null : format("TABLE_SCHEMA LIKE '%s'", schemaPattern);
		String where = Stream.of(catalogClause, schemaClause).filter(Objects::nonNull).collect(joining(" AND "));
		if (!where.isEmpty()) {
			where = " WHERE " + where;
		}
		return getSchemas("SELECT DISTINCT TABLE_SCHEMA AS TABLE_SCHEM, TABLE_CATALOG FROM information_schema.tables" + where);
	}

	private ResultSet getSchemas(String query) throws SQLException {
		List<List<?>> rows = new ArrayList<>();
		try (Statement statement = connection.createStatement();
			 ResultSet schemaDescription = statement.executeQuery(query)) {
			while (schemaDescription.next()) {
				rows.add(List.of(schemaDescription.getString(TABLE_SCHEM), schemaDescription.getString(TABLE_CATALOG)));
			}
		}
		return createResultSet(Stream.of(entry(TABLE_SCHEM, TEXT), entry(TABLE_CATALOG, TEXT)), rows);
	}

	@Override
	public ResultSet getTableTypes() throws SQLException {
		return createResultSet(Stream.of(entry(TABLE_TYPE, TEXT)), List.of(List.of(TABLE), List.of(VIEW)));
	}

	@Override
	public ResultSet getCatalogs() throws SQLException {
		List<List<?>> rows = new ArrayList<>();
		String query = "SELECT CATALOG_NAME AS TABLE_CAT FROM information_schema.catalogs ORDER BY TABLE_CAT";
		try (Statement statement = connection.createStatement();
			 ResultSet catalogNames = statement.executeQuery(query)) {
			while (catalogNames.next()) {
				rows.add(List.of(catalogNames.getString(TABLE_CAT)));
			}
		}
		return createResultSet(Stream.of(entry(TABLE_CAT, TEXT)), rows);
	}

	@Override
	public Connection getConnection() throws SQLException {
		return connection;
	}

	@Override
	public String getDatabaseProductName() {
		return "Firebolt";
	}

	@Override
	public String getURL() {
		return url;
	}

	@Override
	public String getDriverName() {
		return "Firebolt JDBC Driver";
	}

	@Override
	public boolean supportsTransactionIsolationLevel(int level) {
		return level == Connection.TRANSACTION_NONE;
	}

	@Override
	public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
			throws SQLException {
		List<List<?>> rows = new ArrayList<>();
		String query = MetadataUtil.getColumnsQuery(catalog, schemaPattern, tableNamePattern, columnNamePattern);
		try (Statement statement = connection.createStatement();
				ResultSet columnDescription = statement.executeQuery(query)) {
			while (columnDescription.next()) {
				Column columnInfo = Column.of(columnDescription.getString("data_type"),
						columnDescription.getString("column_name"));
				String columnDefault = columnDescription.getString("column_default");
				rows.add(Arrays.asList(connection.getCatalog(), // TABLE_CAT
						columnDescription.getString("table_schema"), // schema
						columnDescription.getString("table_name"), // table name
						columnDescription.getString("column_name"), // column name
						String.valueOf(columnInfo.getType().getDataType().getSqlType()), // sql data type
						columnInfo.getType().getCompactTypeName(), // shorter type name
						String.valueOf(columnInfo.getType().getPrecision()),// column size
						null, // buffer length (not used, see Javadoc)
						String.valueOf(columnInfo.getType().getScale()), // DECIMAL_DIGITS
						String.valueOf(COMMON_RADIX), // radix
						isColumnNullable(columnDescription) ? columnNullable : columnNoNulls,
						null, // description of the column
						columnDefault == null || columnDefault.isBlank() ? null : columnDefault,
						null, // SQL_DATA_TYPE - reserved for future use (see javadoc)
						null, // SQL_DATETIME_SUB - reserved for future use (see javadoc)
						null, // CHAR_OCTET_LENGTH - The maximum
						// length of binary and character
						// based columns (null for others)
						columnDescription.getInt("ordinal_position"), // The ordinal position starting from 1
						isColumnNullable(columnDescription) ? "YES" : "NO",
						null, // "SCOPE_CATALOG - Unused
						null, // "SCOPE_SCHEMA" - Unused
						null, // "SCOPE_TABLE" - Unused
						null, // "SOURCE_DATA_TYPE" - Unused
						"NO", // IS_AUTOINCREMENT - Not supported
						"NO")); // IS_GENERATEDCOLUMN - Not supported
			}
			return createResultSet(
					Stream.of(
							entry(TABLE_CAT, TEXT),
							entry(TABLE_SCHEM, TEXT),
							entry(TABLE_NAME, TEXT),
							entry(COLUMN_NAME, TEXT),
							entry(DATA_TYPE, INTEGER),
							entry(TYPE_NAME, TEXT),
							entry(COLUMN_SIZE, INTEGER),
							entry(BUFFER_LENGTH, INTEGER),
							entry(DECIMAL_DIGITS, INTEGER),
							entry(NUM_PREC_RADIX, INTEGER),
							entry(NULLABLE, INTEGER),
							entry(REMARKS, TEXT),
							entry(COLUMN_DEF, TEXT),
							entry(SQL_DATA_TYPE, INTEGER),
							entry(SQL_DATETIME_SUB, INTEGER),
							entry(CHAR_OCTET_LENGTH, INTEGER),
							entry(ORDINAL_POSITION, INTEGER),
							entry(IS_NULLABLE, TEXT),
							entry(SCOPE_CATALOG, TEXT),
							entry(SCOPE_SCHEMA, TEXT),
							entry(SCOPE_TABLE, TEXT),
							entry(SOURCE_DATA_TYPE, INTEGER),
							entry(IS_AUTOINCREMENT, TEXT),
							entry(IS_GENERATEDCOLUMN, TEXT)),
					rows);
		}
	}

	private boolean isColumnNullable(ResultSet columnDescription) throws SQLException {
		final String isNullable = "is_nullable";
		try {
			return columnDescription.getInt(isNullable) == 1;
		} catch (SQLException | NumberFormatException e1) {
			try {
				return columnDescription.getBoolean(isNullable);
			} catch (SQLException e2) {
				return "YES".equals(columnDescription.getString(isNullable));
			}
		}
	}

	@Override
	public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] typesArr) throws SQLException {
		return createResultSet(
				Stream.of(TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, REMARKS, TYPE_CAT, TYPE_SCHEM, TYPE_NAME, SELF_REFERENCING_COL_NAME, REF_GENERATION)
						.map(name -> entry(name, TEXT)),
				getTablesData(catalog, schemaPattern, tableNamePattern, typesArr));
	}

	private List<List<?>> getTablesData(String catalog, String schemaPattern, String tableNamePattern, String[] typesArr) throws SQLException {
		Set<String> types = typesArr == null ? Set.of(TABLE, VIEW) : Set.of(typesArr);
		List<String> tableTypes = List.of("BASE TABLE", "DIMENSION", "FACT");
		List<String> trulyTableTypes = new ArrayList<>();
		if (types.contains(TABLE)) {
			trulyTableTypes.addAll(tableTypes);
		}
		if (types.contains(VIEW)) {
			trulyTableTypes.add(VIEW);
		}
		String query = MetadataUtil.getTablesQuery(catalog, schemaPattern, tableNamePattern, trulyTableTypes.toArray(new String[0]));
		List<List<?>> rows = new ArrayList<>();
		try (Statement statement = connection.createStatement(); ResultSet tables = statement.executeQuery(query)) {
			while (tables.next()) {
				String tableType = tables.getString("table_type");
				tableType = tableTypes.contains(tableType) ? TABLE: tableType; // replace FACT and DIMENSION by TABLE
				List<String> row = Arrays.asList(
						connection.getCatalog(), tables.getString("table_schema"), tables.getString("table_name"), tableType,
						null, null, null, null, null, null);
				rows.add(row);
			}
		}
		return rows;
	}

	@Override
	public ResultSet getTypeInfo() throws SQLException {
		List<FireboltDataType> usableTypes = Arrays.asList(INTEGER, BIG_INT, REAL, DOUBLE_PRECISION, TEXT, DATE,
				TIMESTAMP, NUMERIC, ARRAY, TUPLE, BYTEA, BOOLEAN);
		List<List<?>> rows = usableTypes.stream().map(type ->
				Arrays.asList(type.getDisplayName(), type.getSqlType(),
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
						COMMON_RADIX))
				.collect(toList());

		return createResultSet(
				Stream.of(
						entry(TYPE_NAME, TEXT),
						entry(DATA_TYPE, INTEGER),
						entry(PRECISION, INTEGER),
						entry(LITERAL_PREFIX, TEXT),
						entry(LITERAL_SUFFIX, TEXT),
						entry(CREATE_PARAMS, TEXT),
						entry(NULLABLE, INTEGER),
						entry(CASE_SENSITIVE, BOOLEAN),
						entry(SEARCHABLE, INTEGER),
						entry(UNSIGNED_ATTRIBUTE, BOOLEAN),
						entry(FIXED_PREC_SCALE, BOOLEAN),
						entry(AUTO_INCREMENT, BOOLEAN),
						entry(LOCAL_TYPE_NAME, TEXT),
						entry(MINIMUM_SCALE, INTEGER),
						entry(MAXIMUM_SCALE, INTEGER),
						entry(SQL_DATA_TYPE, INTEGER),
						entry(SQL_DATETIME_SUB, INTEGER),
						entry(NUM_PREC_RADIX, INTEGER)),
				rows);
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
		if (databaseVersion == null) {
			try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery("SELECT VERSION()")) {
				databaseVersion = rs.next() ? rs.getString(1) : "";
			}
		}
		return databaseVersion;
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
	public int getJDBCMajorVersion() {
		return VersionUtil.extractMajorVersion(VersionUtil.getSpecificationVersion());
	}

	@Override
	public int getJDBCMinorVersion() {
		return VersionUtil.extractMinorVersion(VersionUtil.getSpecificationVersion());
	}

	@Override
	public String getDriverVersion() {
		return VersionUtil.getDriverVersion();
	}

	@Override
	public boolean allProceduresAreCallable() {
		return false;
	}

	@Override
	public boolean allTablesAreSelectable() {
		return true;
	}

	@Override
	public String getUserName() {
		return connection.getSessionProperties().getPrincipal();
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		return false;
	}

	@Override
	public boolean nullsAreSortedHigh() {
		return false;
	}

	@Override
	public boolean nullsAreSortedLow() {
		return !nullsAreSortedHigh();
	}

	@Override
	public boolean nullsAreSortedAtStart() {
		return false;
	}

	@Override
	public boolean nullsAreSortedAtEnd() {
		return !nullsAreSortedAtStart();
	}

	@Override
	public boolean usesLocalFiles() {
		return false;
	}

	@Override
	public boolean usesLocalFilePerTable() {
		return false;
	}

	@Override
	public boolean supportsMixedCaseIdentifiers() {
		return false;
	}

	@Override
	public boolean storesUpperCaseIdentifiers() {
		return false;
	}

	@Override
	public boolean storesLowerCaseIdentifiers() {
		return true;
	}

	@Override
	public boolean storesMixedCaseIdentifiers() {
		return false;
	}

	@Override
	public boolean supportsMixedCaseQuotedIdentifiers() {
		return true;
	}

	@Override
	public boolean storesUpperCaseQuotedIdentifiers() {
		return false;
	}

	@Override
	public boolean storesLowerCaseQuotedIdentifiers() {
		return false;
	}

	@Override
	public boolean storesMixedCaseQuotedIdentifiers() {
		return true;
	}

	@Override
	public String getIdentifierQuoteString() {
		return "\"";
	}

	@Override
	public String getSQLKeywords() {
		// Firebolt reserved words minus SQL:2003 keywords
		return "ACCOUNT,AGGREGATING,ALTER,AS,ATTACH,CACHE,CANCEL,COLUMNS,COPY,CREATE,DATABASE,DATABASES,DELETE,DESCRIBE,DROP,"
				+ "ENGINE,ENGINES,EXTERNAL,GRANT,INDEX,INDEXES,INSERT,LOGIN,NETWORK,ORGANIZATION,POLICY,QUERY,REVOKE,ROLE,"
				+ "SELECT,SERVICE,SHOW,START,STOP,TABLE,TABLES,TO,TRUNCATE,UPDATE,USER,VACUUM,VIEW,VIEWS";
	}

	@Override
	public String getNumericFunctions() {
		return "ABS,ACOS,ASIN,ATAN,ATAN2,CBRT,CEIL,COS,COT,DEGREES,EXP,FLOOR,LOG,MOD,PI,POW,RADIANS,RANDOM,ROUND,SIGN,SIN,SQRT,TAN,TRUNC";
	}

	@Override
	public String getStringFunctions() {
		return "ARRAY_ENUMERATE,BASE64_ENCODE,BTRIM,CONCAT,EXTRACT_ALL,GEN_RANDOM_UUID,ILIKE,LENGTH,LIKE,LOWER,LPAD,LTRIM,MATCH,MATCH_ANY,"
				+ "MD5,MD5_NUMBER_LOWER64,MD5_NUMBER_UPPER64,OCTET_LENGTH,REGEXP_LIKE,REGEXP_MATCHES,REGEXP_REPLACE,REPEAT,REPLACE,REVERSE,"
				+ "RPAD,RTRIM,SPLIT,SPLIT_PART,STRPOS,SUBSTRING,SUBSTR,TO_DOUBLE,TO_FLOAT,TO_INT,TRIM,UPPER,URL_DECODE,URL_ENCODE";
	}

	@Override
	public String getSystemFunctions() {
		return "VERSION";
	}

	@Override
	public String getTimeDateFunctions() {
		return "CURRENT_DATE,CURRENT_TIMESTAMP,DATE_ADD,DATE_DIFF,DATE_TRUNC,EXTRACT,TO_CHAR,TO_DATE,TO_TIMESTAMP,TO_TIMESTAMPTZ";
	}

	@Override
	public String getSearchStringEscape() {
		return "\\";
	}

	/**
	 * Returns empty string for compatibility with PostgreSQL.
	 * @return empty string
	 */
	@Override
	public String getExtraNameCharacters() {
		return "";
	}

	@Override
	public boolean supportsAlterTableWithAddColumn() {
		return false;
	}

	@Override
	public boolean supportsAlterTableWithDropColumn() {
		return false;
	}

	@Override
	public boolean supportsColumnAliasing() {
		return true;
	}

	@Override
	public boolean nullPlusNonNullIsNull() {
		return true;
	}

	@Override
	public boolean supportsConvert() {
		return false;
	}

	@Override
	public boolean supportsConvert(int fromType, int toType) {
		return false;
	}

	@Override
	public boolean supportsTableCorrelationNames() {
		return true;
	}

	@Override
	public boolean supportsDifferentTableCorrelationNames() {
		return false;
	}

	@Override
	public boolean supportsExpressionsInOrderBy() {
		return true;
	}

	@Override
	public boolean supportsOrderByUnrelated() {
		return true;
	}

	@Override
	public boolean supportsGroupBy() {
		return true;
	}

	@Override
	public boolean supportsGroupByUnrelated() {
		return false;
	}

	@Override
	public boolean supportsGroupByBeyondSelect() {
		return false;
	}

	@Override
	public boolean supportsLikeEscapeClause() {
		return false;
	}

	@Override
	public boolean supportsMultipleResultSets() {
		return false;
	}

	@Override
	public boolean supportsMultipleTransactions() {
		return false;
	}

	@Override
	public boolean supportsNonNullableColumns() {
		return true;
	}

	/**
	 * {@inheritDoc}
	 *  <p>This grammar is defined at:
     * <a href="https://learn.microsoft.com/en-us/sql/odbc/reference/appendixes/sql-minimum-grammar?view=sql-server-ver16">SQL Minimum Grammar</a>
     * @return true
     * @throws SQLException - actually never throws
     */
	@Override
	public boolean supportsMinimumSQLGrammar() {
		return true;
	}

	/**
	 * Does this driver support the Core ODBC SQL grammar. We need SQL-92 conformance for this.
	 *
	 * @return false
	 */
	@Override
	public boolean supportsCoreSQLGrammar() {
		return false;
	}

	@Override
	public boolean supportsExtendedSQLGrammar() {
		return false;
	}

	@Override
	public boolean supportsANSI92EntryLevelSQL() {
		// We do not support it (eg: we would need to be compliant with JDBC and support 'schema')
		return false;
	}

	@Override
	public boolean supportsANSI92IntermediateSQL() {
		return false;
	}

	@Override
	public boolean supportsANSI92FullSQL() {
		return false;
	}

	@Override
	public boolean supportsIntegrityEnhancementFacility() {
		// Similar approach as pgjdbc: we assume it means supported constraints
		return false;
	}

	@Override
	public boolean supportsOuterJoins() {
		return true;
	}

	@Override
	public boolean supportsFullOuterJoins() {
		return true;
	}

	@Override
	public boolean supportsLimitedOuterJoins() {
		return true;
	}

	@Override
	public String getSchemaTerm() {
		return "schema";
	}

	@Override
	public String getProcedureTerm() {
		return "procedure";
	}

	@Override
	public String getCatalogTerm() {
		return "database";
	}

	@Override
	public boolean isCatalogAtStart() {
		// it is currently not supported but it will be soon
		return false;
	}

	@Override
	public String getCatalogSeparator() {
		return ".";
	}

	@Override
	public boolean supportsSchemasInDataManipulation() {
		return false;
	}

	@Override
	public boolean supportsSchemasInProcedureCalls() {
		return false;
	}

	@Override
	public boolean supportsSchemasInTableDefinitions() {
		return false;
	}

	@Override
	public boolean supportsSchemasInIndexDefinitions() {
		return false;
	}

	@Override
	public boolean supportsSchemasInPrivilegeDefinitions() {
		return false;
	}

	@Override
	public boolean supportsCatalogsInDataManipulation() {
		return false;
	}

	@Override
	public boolean supportsCatalogsInProcedureCalls() {
		return false;
	}

	@Override
	public boolean supportsCatalogsInTableDefinitions() {
		return false;
	}

	@Override
	public boolean supportsCatalogsInIndexDefinitions() {
		return false;
	}

	@Override
	public boolean supportsCatalogsInPrivilegeDefinitions() {
		return false;
	}

	@Override
	public boolean supportsPositionedDelete() {
		return false;
	}

	@Override
	public boolean supportsPositionedUpdate() {
		return false;
	}

	@Override
	public boolean supportsSelectForUpdate() {
		return false;
	}

	@Override
	public boolean supportsStoredProcedures() {
		return false;
	}

	@Override
	public boolean supportsSubqueriesInComparisons() {
		return true;
	}

	@Override
	public boolean supportsSubqueriesInExists() {
		return true;
	}

	@Override
	public boolean supportsSubqueriesInIns() {
		return true;
	}

	@Override
	public boolean supportsSubqueriesInQuantifieds() {
		return false;
	}

	@Override
	public boolean supportsCorrelatedSubqueries() {
		return true;
	}

	@Override
	public boolean supportsUnion() {
		return true;
	}

	@Override
	public boolean supportsUnionAll() {
		return true;
	}

	@Override
	public boolean supportsOpenCursorsAcrossCommit() {
		return false;
	}

	@Override
	public boolean supportsOpenCursorsAcrossRollback() {
		return false;
	}

	@Override
	public boolean supportsOpenStatementsAcrossCommit() {
		return false;
	}

	@Override
	public boolean supportsOpenStatementsAcrossRollback()  {
		return false;
	}

	@Override
	@SuppressWarnings("java:S4144") // identical implementation
	public int getMaxBinaryLiteralLength() {
		return MAX_LITERAL_LENGTH;
	}

	@Override
	@SuppressWarnings("java:S4144") // identical implementation
	public int getMaxCharLiteralLength() {
		return MAX_LITERAL_LENGTH;
	}

	@Override
	@SuppressWarnings("java:S4144") // identical implementation
	public int getMaxColumnNameLength() {
		return MAX_IDENTIFIER_LENGTH;
	}

	@Override
	public int getMaxColumnsInGroupBy() {
		return 0x10000; //65536
	}

	/**
	 * Indexes are not supported, so the value is irrelevant.
	 * @return 0
	 */
	public int getMaxColumnsInIndex() {
		return 0;
	}

	@Override
	public int getMaxColumnsInOrderBy() {
		return 16384;
	}

	@Override
	public int getMaxColumnsInSelect() {
		return 8192;
	}

	@Override
	public int getMaxColumnsInTable() {
		return 1000;
	}

	@Override
	public int getMaxConnections() {
		return 0;
	}

	@Override
	public int getMaxCursorNameLength() {
		return 0;
	}

	@Override
	public int getMaxIndexLength() {
		return 0;
	}

	@Override
	@SuppressWarnings("java:S4144") // identical implementation
	public int getMaxSchemaNameLength() {
		return MAX_IDENTIFIER_LENGTH;
	}

	@Override
	public int getMaxProcedureNameLength() {
		return 0;
	}

	@Override
	@SuppressWarnings("java:S4144") // identical implementation
	public int getMaxCatalogNameLength() {
		return MAX_IDENTIFIER_LENGTH;
	}

	@Override
	public int getMaxRowSize() {
		return 0;
	}

	@Override
	public boolean doesMaxRowSizeIncludeBlobs() {
		return true;
	}

	@Override
	public int getMaxStatementLength() {
		return 0;
	}

	@Override
	public int getMaxStatements() {
		return 0;
	}

	@Override
	@SuppressWarnings("java:S4144") // identical implementation
	public int getMaxTableNameLength() {
		return MAX_IDENTIFIER_LENGTH;
	}

	@Override
	public int getMaxTablesInSelect() {
		return 0;
	}

	@Override
	@SuppressWarnings("java:S4144") // identical implementation
	public int getMaxUserNameLength() {
		return MAX_IDENTIFIER_LENGTH;
	}

	@Override
	public int getDefaultTransactionIsolation() {
		return Connection.TRANSACTION_NONE;
	}

	@Override
	public boolean supportsTransactions() {
		return false;
	}

	@Override
	public boolean supportsDataDefinitionAndDataManipulationTransactions() {
		return false;
	}

	@Override
	public boolean supportsDataManipulationTransactionsOnly() {
		return false;
	}

	@Override
	public boolean dataDefinitionCausesTransactionCommit() {
		return false;
	}

	@Override
	public boolean dataDefinitionIgnoredInTransactions() {
		return false;
	}

	@Override
	public boolean supportsResultSetType(int type) {
		return TYPE_FORWARD_ONLY == type;
	}

	@Override
	public boolean supportsResultSetConcurrency(int type, int concurrency) {
		return type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY;
	}

	/*
	 * The methods related to the visibility of updated ResultSets do not apply
	 * since we do not support updating ResultSet objects
	 */
	@Override
	public boolean ownUpdatesAreVisible(int type) {
		return false;
	}

	@Override
	public boolean ownDeletesAreVisible(int type) {
		return false;
	}

	@Override
	public boolean ownInsertsAreVisible(int type) {
		return false;
	}

	@Override
	public boolean othersUpdatesAreVisible(int type) {
		return false;
	}

	@Override
	public boolean othersDeletesAreVisible(int type) {
		return false;
	}

	@Override
	public boolean othersInsertsAreVisible(int type) {
		return false;
	}

	@Override
	public boolean updatesAreDetected(int type) {
		return false;
	}

	@Override
	public boolean deletesAreDetected(int type) {
		return false;
	}

	@Override
	public boolean insertsAreDetected(int type) {
		return false;
	}

	@Override
	public boolean supportsBatchUpdates() {
		// We support it partially (via FireboltPreparedStatement but not with the
		// 'basic' FireboltStatement )
		return false;
	}

	@Override
	public boolean supportsSavepoints() {
		return false;
	}

	@Override
	public boolean supportsNamedParameters() {
		return false;
	}

	@Override
	public boolean supportsMultipleOpenResults() {
		return false;
	}

	@Override
	public boolean supportsGetGeneratedKeys() {
		return false;
	}

	@Override
	public boolean supportsResultSetHoldability(int holdability) {
		return holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
	}

	/**
	 * Since Firebolt does not support transactions commit does not affect the existing {@code ResultSet} and therefore
	 * it behaves as if it is held between transaction. Therefore, it returns {@link ResultSet#HOLD_CURSORS_OVER_COMMIT}
	 * @return {@link ResultSet#HOLD_CURSORS_OVER_COMMIT}
	 */
	@Override
	public int getResultSetHoldability() {
		return ResultSet.HOLD_CURSORS_OVER_COMMIT;
	}

	@Override
	public int getSQLStateType() {
		return sqlStateSQL;
	}

	@Override
	public boolean locatorsUpdateCopy() {
		return false;
	}

	@Override
	public boolean supportsStatementPooling() {
		return false;
	}

	@Override
	public RowIdLifetime getRowIdLifetime() {
		return RowIdLifetime.ROWID_UNSUPPORTED;
	}

	@Override
	public boolean supportsStoredFunctionsUsingCallSyntax() {
		return false;
	}

	@Override
	public boolean autoCommitFailureClosesAllResultSets() {
		return false;
	}

	@Override
	public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
		return createEmptyResultSet(Stream.of(
				entry(PROCEDURE_CAT, TEXT),
				entry(PROCEDURE_SCHEM, TEXT),
				entry(PROCEDURE_NAME, TEXT),
				entry(COLUMN_NAME, TEXT),
				entry(COLUMN_TYPE, INTEGER), // Short
				entry(DATA_TYPE, INTEGER),
				entry(TYPE_NAME, TEXT),
				entry(PRECISION, INTEGER),
				entry(LENGTH, INTEGER),
				entry(SCALE, INTEGER), // short
				entry(RADIX, INTEGER), // short
				entry(NULLABLE, INTEGER), // short
				entry(REMARKS, TEXT),
				entry(COLUMN_DEF, TEXT),
				entry(SQL_DATA_TYPE, INTEGER),
				entry(SQL_DATETIME_SUB, INTEGER),
				entry(CHAR_OCTET_LENGTH, INTEGER),
				entry(ORDINAL_POSITION, INTEGER),
				entry(IS_NULLABLE, TEXT),
				entry(SPECIFIC_NAME, TEXT)
		));
	}

	@Override
	public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
		return createEmptyResultSet(Stream.of(
				entry(TYPE_CAT, TEXT),
				entry(TYPE_SCHEM, TEXT),
				entry(TYPE_NAME, TEXT),
				entry(CLASS_NAME, TEXT),
				entry(DATA_TYPE, INTEGER),
				entry(REMARKS, TEXT),
				entry(BASE_TYPE, INTEGER) // short
		));
	}

	@Override
	public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
		return createEmptyResultSet(Stream.of(
				entry(TYPE_CAT, TEXT),
				entry(TYPE_SCHEM, TEXT),
				entry(TYPE_NAME, TEXT),
				entry(SUPERTYPE_CAT, TEXT),
				entry(SUPERTYPE_SCHEM, TEXT),
				entry(SUPERTYPE_NAME, TEXT)
		));
	}

	@Override
	public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
		return createEmptyResultSet(Stream.of(
				entry(TYPE_CAT, TEXT),
				entry(TYPE_SCHEM, TEXT),
				entry(TYPE_NAME, TEXT),
				entry(SUPERTABLE_NAME, TEXT)
		));
	}

	@Override
	public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
		return createEmptyResultSet(Stream.of(
				entry(TYPE_CAT, TEXT),
				entry(TYPE_SCHEM, TEXT),
				entry(TYPE_NAME, TEXT),
				entry(ATTR_NAME, TEXT),
				entry(DATA_TYPE, INTEGER),
				entry(ATTR_TYPE_NAME, TEXT),
				entry(ATTR_SIZE, INTEGER),
				entry(DECIMAL_DIGITS, INTEGER),
				entry(NUM_PREC_RADIX, INTEGER),
				entry(NULLABLE, INTEGER),
				entry(REMARKS, TEXT),
				entry(ATTR_DEF, TEXT),
				entry(SQL_DATA_TYPE, INTEGER),
				entry(SQL_DATETIME_SUB, INTEGER),
				entry(CHAR_OCTET_LENGTH, INTEGER),
				entry(ORDINAL_POSITION, INTEGER),
				entry(IS_NULLABLE, TEXT),
				entry(SCOPE_CATALOG, TEXT),
				entry(SCOPE_SCHEMA, TEXT),
				entry(SCOPE_TABLE, TEXT),
				entry(SOURCE_DATA_TYPE, INTEGER) // short
		));
	}

	@Override
	public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
		return createEmptyResultSet(Stream.of(
				entry(PROCEDURE_CAT, TEXT),
				entry(PROCEDURE_SCHEM, TEXT),
				entry(PROCEDURE_NAME, TEXT),
				entry(REMARKS, TEXT),
				entry(PROCEDURE_TYPE, INTEGER), // short
				entry(SPECIFIC_NAME, TEXT)
		));
	}

	@Override
	public ResultSet getColumnPrivileges(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
		List<List<?>> rows = new ArrayList<>();
		String query = MetadataUtil.getColumnsQuery(catalog, schemaPattern, tableNamePattern, columnNamePattern);
		try (Statement statement = connection.createStatement();
			 ResultSet columnDescription = statement.executeQuery(query)) {
			while (columnDescription.next()) {
				rows.add(Arrays.asList(connection.getCatalog(),
						columnDescription.getString("table_schema"),
						columnDescription.getString("table_name"),
						columnDescription.getString("column_name"),
						null,  // grantor
						null,  // grantee
						null,  // privilege
						"NO")); // is_grantable
			}
		}
		return createResultSet(Stream.of(
				entry(TABLE_CAT, TEXT),
				entry(TABLE_SCHEM, TEXT),
				entry(TABLE_NAME, TEXT),
				entry(COLUMN_NAME, TEXT),
				entry(GRANTOR, TEXT),
				entry(GRANTEE, TEXT),
				entry(PRIVILEGE, TEXT),
				entry(IS_GRANTABLE, TEXT)
		), rows);
	}

	@Override
	public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
		List<List<?>> rows = new ArrayList<>();
		String query = MetadataUtil.getTablesQuery(catalog, schemaPattern, tableNamePattern, new String[] {"BASE TABLE", "DIMENSION", "FACT"});
		try (Statement statement = connection.createStatement();
			 ResultSet columnDescription = statement.executeQuery(query)) {
			while (columnDescription.next()) {
				rows.add(Arrays.asList(connection.getCatalog(),
						columnDescription.getString("table_schema"),
						columnDescription.getString("table_name"),
						null,  // grantor
						null,  // grantee
						null,  // privilege
						"NO")); // is_grantable
			}
		}
		return createResultSet(Stream.of(
				entry(TABLE_CAT, TEXT),
				entry(TABLE_SCHEM, TEXT),
				entry(TABLE_NAME, TEXT),
				entry(GRANTOR, TEXT),
				entry(GRANTEE, TEXT),
				entry(PRIVILEGE, TEXT),
				entry(IS_GRANTABLE, TEXT)
		),  rows);
	}

	@Override
	public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
		return createEmptyResultSet(Stream.of(
				entry(SCOPE, INTEGER), // short
				entry(COLUMN_NAME, TEXT),
				entry(DATA_TYPE, INTEGER),
				entry(TYPE_NAME, TEXT),
				entry(COLUMN_SIZE, INTEGER),
				entry(BUFFER_LENGTH, INTEGER),
				entry(DECIMAL_DIGITS, INTEGER), // short
				entry(PSEUDO_COLUMN, INTEGER) // short
		));
	}

	@Override
	public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
		return createEmptyResultSet(Stream.of(
				entry(SCOPE, INTEGER), // short
				entry(COLUMN_NAME, TEXT),
				entry(DATA_TYPE, INTEGER),
				entry(TYPE_NAME, TEXT),
				entry(COLUMN_SIZE, INTEGER),
				entry(BUFFER_LENGTH, INTEGER),
				entry(DECIMAL_DIGITS, INTEGER), // short
				entry(PSEUDO_COLUMN, INTEGER) // short
		));
	}

	@Override
	public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
		return createEmptyResultSet(Stream.of(
				entry(TABLE_CAT, TEXT),
				entry(TABLE_SCHEM, TEXT),
				entry(TABLE_NAME, TEXT),
				entry(COLUMN_NAME, TEXT),
				entry(KEY_SEQ, INTEGER), // short
				entry(PK_NAME, TEXT)
		));
	}

	@Override
	public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
		return getKeys();
	}

	@Override
	public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
		return getKeys();
	}
	@Override
	public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
			String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
		return getKeys();
	}

	private ResultSet getKeys() throws SQLException {
		return createEmptyResultSet(Stream.of(
				entry(PKTABLE_CAT, TEXT),
				entry(PKTABLE_SCHEM, TEXT),
				entry(PKTABLE_NAME, TEXT),
				entry(PKCOLUMN_NAME, TEXT),
				entry(FKTABLE_CAT, TEXT),
				entry(FKTABLE_SCHEM, TEXT),
				entry(FKTABLE_NAME, TEXT),
				entry(FKCOLUMN_NAME, TEXT),
				entry(KEY_SEQ, INTEGER), // short
				entry(UPDATE_RULE, INTEGER), // short
				entry(DELETE_RULE, INTEGER), // short
				entry(FK_NAME, TEXT),
				entry(PK_NAME, TEXT),
				entry(DEFERRABILITY, INTEGER) // short
		));
	}

	@Override
	public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
		return createEmptyResultSet(Stream.of(
				entry(TABLE_CAT, TEXT),
				entry(TABLE_SCHEM, TEXT),
				entry(TABLE_NAME, TEXT),
				entry(NON_UNIQUE, BOOLEAN),
				entry(INDEX_QUALIFIER, TEXT),
				entry(INDEX_NAME, TEXT),
				entry(TYPE, INTEGER), // short
				entry(ORDINAL_POSITION, INTEGER), // short
				entry(COLUMN_NAME, TEXT),
				entry(ASC_OR_DESC, TEXT),
				entry(CARDINALITY, BIG_INT), // long
				entry(PAGES, BIG_INT), // long
				entry(FILTER_CONDITION, INTEGER)
		));
	}

	@Override
	public ResultSet getClientInfoProperties() throws SQLException {
		return createEmptyResultSet(Stream.of(
				entry(NAME, TEXT),
				entry(MAX_LEN, INTEGER),
				entry(DEFAULT_VALUE, TEXT),
				entry(DESCRIPTION, TEXT)
		));
	}

	@Override
	public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
		Predicate<String> functionFilter = functionNamePattern == null ? f -> true : compile(functionNamePattern, CASE_INSENSITIVE).asPredicate();

		List<List<?>> rows = Arrays.stream(String.join(",", getStringFunctions(), getNumericFunctions(), getTimeDateFunctions(), getSystemFunctions()).split(","))
				.map(String::trim) // instead of split("\\s*,\\s") blocked by Sonar according to its opinion "can lead denial of service" (?!)
				.filter(functionFilter)
				.sorted()
				.distinct() // some functions belong to different categories, e.g. TO_DATE is both date-time and string function
				.map(function -> Arrays.asList(null, null, function, null, functionNoTable, function))
				.collect(toList());
		return createResultSet(Stream.of(
				entry(FUNCTION_CAT, TEXT),
				entry(FUNCTION_SCHEM, TEXT),
				entry(FUNCTION_NAME, TEXT),
				entry(REMARKS, TEXT),
				entry(FUNCTION_TYPE, INTEGER),
				entry(SPECIFIC_NAME, TEXT)), rows);
	}

	@Override
	public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
		Predicate<String> functionFilter = functionNamePattern == null ? f -> true : compile(functionNamePattern, CASE_INSENSITIVE).asPredicate();

		List<List<?>> stringFunctions = Arrays.stream(String.join(",", getStringFunctions()).split(",")).map(String::trim).filter(functionFilter)
				.map(function -> Arrays.asList(null, null, function, null, functionColumnUnknown, Types.VARCHAR, JDBCType.VARCHAR.getName(), null, null, null, null, functionNullable, null, null, null, "YES", function))
				.collect(toList());

		List<List<?>> numericFunctions = Arrays.stream(String.join(",", getNumericFunctions()).split(",")).map(String::trim).filter(functionFilter)
				.map(function -> Arrays.asList(null, null, function, null, functionColumnUnknown, Types.INTEGER, JDBCType.INTEGER, null, null, null, null, functionNullableUnknown, null, null, null, "", function))
				.collect(toList());

		List<List<?>> timeDateFunctions = Arrays.stream(String.join(",", getTimeDateFunctions()).split(",")).map(String::trim).filter(functionFilter)
				.map(function -> {
					int type = Types.OTHER;
					if (function.contains("TZ")) {
						type = Types.TIMESTAMP_WITH_TIMEZONE;
					} else if (function.contains("TIMESTAMP")) {
						type = Types.TIMESTAMP;
					} else if (function.contains("DATE")) {
						type = Types.DATE;
					}
					return Arrays.asList(null, null, function, null, functionColumnUnknown, type, JDBCType.valueOf(type), null, null, null, null, functionNullableUnknown, null, null, null, "", function);
				})
				.collect(toList());

		List<List<?>> systemFunctions = Arrays.stream(String.join(",", getSystemFunctions()).split(",")).map(String::trim).filter(functionFilter)
				.map(function -> Arrays.asList(null, null, function, null, functionColumnUnknown, VARCHAR, JDBCType.VARCHAR.getName(), null, null, null, null, functionNullableUnknown, null, null, null, "", function))
				.collect(toList());

		Comparator<List<?>> comparator = (row1, row2) -> {
			String function1 = (String)row1.get(2);
			String function2 = (String)row2.get(2);
			return function1.compareToIgnoreCase(function2);
		};
		List<List<?>> allFunctions = Stream.of(stringFunctions, numericFunctions, timeDateFunctions, systemFunctions).flatMap(Collection::stream).sorted(comparator).collect(toList());

		return createResultSet(Stream.of(
				entry(FUNCTION_CAT, TEXT),
				entry(FUNCTION_SCHEM, TEXT),
				entry(FUNCTION_NAME, TEXT),
				entry(COLUMN_NAME, TEXT),
				entry(COLUMN_TYPE, INTEGER),
				entry(DATA_TYPE, INTEGER),
				entry(TYPE_NAME, TEXT),
				entry(PRECISION, INTEGER),
				entry(LENGTH, INTEGER),
				entry(SCALE, INTEGER),
				entry(RADIX, INTEGER),
				entry(NULLABLE, INTEGER),
				entry(REMARKS, TEXT),
				entry(CHAR_OCTET_LENGTH, INTEGER),
				entry(ORDINAL_POSITION, INTEGER),
				entry(IS_NULLABLE, TEXT),
				entry(SPECIFIC_NAME, TEXT)), allFunctions);
	}
	@Override
	public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
		return createEmptyResultSet(Stream.of(
				entry(TABLE_CAT, TEXT),
				entry(TABLE_SCHEM, TEXT),
				entry(TABLE_NAME, TEXT),
				entry(COLUMN_NAME, TEXT),
				entry(DATA_TYPE, INTEGER),
				entry(COLUMN_SIZE, INTEGER),
				entry(DECIMAL_DIGITS, INTEGER),
				entry(NUM_PREC_RADIX, INTEGER),
				entry(COLUMN_USAGE, TEXT),
				entry(REMARKS, TEXT),
				entry(CHAR_OCTET_LENGTH, INTEGER),
				entry(IS_NULLABLE, TEXT)
		));
	}

	@Override
	public boolean generatedKeyAlwaysReturned() {
		return false;
	}

	private ResultSet createEmptyResultSet(Stream<Map.Entry<String, FireboltDataType>> columns) throws SQLException {
		return createResultSet(columns, List.of());
	}

	private ResultSet createResultSet(Stream<Map.Entry<String, FireboltDataType>> columns, List<List<?>> rows) throws SQLException {
		return FireboltResultSet.of(QueryResult.builder()
				.columns(columns.map(e -> QueryResult.Column.builder().name(e.getKey()).type(e.getValue()).build()).collect(toList()))
				.rows(rows)
				.build());
	}
}
