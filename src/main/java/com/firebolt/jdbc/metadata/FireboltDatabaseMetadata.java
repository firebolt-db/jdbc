package com.firebolt.jdbc.metadata;

import com.firebolt.jdbc.QueryResult;
import com.firebolt.jdbc.annotation.ExcludeFromJacocoGeneratedReport;
import com.firebolt.jdbc.annotation.NotImplemented;
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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.firebolt.jdbc.metadata.MetadataColumns.ATTR_DEF;
import static com.firebolt.jdbc.metadata.MetadataColumns.ATTR_NAME;
import static com.firebolt.jdbc.metadata.MetadataColumns.ATTR_SIZE;
import static com.firebolt.jdbc.metadata.MetadataColumns.ATTR_TYPE_NAME;
import static com.firebolt.jdbc.metadata.MetadataColumns.AUTO_INCREMENT;
import static com.firebolt.jdbc.metadata.MetadataColumns.BASE_TYPE;
import static com.firebolt.jdbc.metadata.MetadataColumns.BUFFER_LENGTH;
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
import static com.firebolt.jdbc.metadata.MetadataColumns.IS_AUTOINCREMENT;
import static com.firebolt.jdbc.metadata.MetadataColumns.IS_GENERATEDCOLUMN;
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
import static com.firebolt.jdbc.metadata.MetadataColumns.NULLABLE;
import static com.firebolt.jdbc.metadata.MetadataColumns.NUM_PREC_RADIX;
import static com.firebolt.jdbc.metadata.MetadataColumns.ORDINAL_POSITION;
import static com.firebolt.jdbc.metadata.MetadataColumns.PKCOLUMN_NAME;
import static com.firebolt.jdbc.metadata.MetadataColumns.PKTABLE_CAT;
import static com.firebolt.jdbc.metadata.MetadataColumns.PKTABLE_NAME;
import static com.firebolt.jdbc.metadata.MetadataColumns.PKTABLE_SCHEM;
import static com.firebolt.jdbc.metadata.MetadataColumns.PK_NAME;
import static com.firebolt.jdbc.metadata.MetadataColumns.PRECISION;
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
import static java.sql.Types.VARCHAR;
import static java.util.Map.entry;
import static java.util.regex.Pattern.CASE_INSENSITIVE;
import static java.util.regex.Pattern.compile;
import static java.util.stream.Collectors.toList;

@CustomLog
@SuppressWarnings("java:S6204") // compatibility with JDK 11
public class FireboltDatabaseMetadata implements DatabaseMetaData {

	private static final String PUBLIC_SCHEMA_NAME = "public";
	private static final String INFORMATION_SCHEMA_NAME = "information_schema";
	private static final String CATALOG_SCHEMA_NAME = "catalog";
	private static final String TABLE = "TABLE";
	private static final String VIEW = "VIEW";
	private static final String QUOTE = "'";
	private static final int MAX_IDENTIFIER_LENGTH = 63;
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
				.columns(List.of(QueryResult.Column.builder().name(TABLE_TYPE).type(TEXT).build()))
				.rows(List.of(List.of(TABLE), List.of(VIEW))).build());
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
	public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] typesArr) throws SQLException {
		return FireboltResultSet.of(QueryResult.builder()
				.columns(Stream.of(TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, REMARKS, TYPE_CAT, TYPE_SCHEM, TYPE_NAME, SELF_REFERENCING_COL_NAME, REF_GENERATION)
						.map(name -> QueryResult.Column.builder().name(name).type(TEXT).build()).collect(toList()))
				.rows(getTablesData(catalog, schemaPattern, tableNamePattern, typesArr)).build());
	}

	private List<List<?>> getTablesData(String catalog, String schemaPattern, String tableNamePattern, String[] typesArr) throws SQLException {
		Set<String> types = typesArr == null ? Set.of(TABLE, VIEW) : Set.of(typesArr);
		Set<String> tableTypes = Set.of("FACT", "DIMENSION");
		List<String> trulyTableTypes = new ArrayList<>();
		if (types.contains(TABLE)) {
			trulyTableTypes.addAll(List.of("FACT", "DIMENSION"));
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

		return FireboltResultSet.of(QueryResult.builder().columns(columns).rows(rows).build());
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
			String engine = connection.getEngine();
			try (Statement statement = connection.createStatement()) {
				String query = MetadataUtil.getDatabaseVersionQuery(engine);
				ResultSet rs = statement.executeQuery(query);
				rs.next();
				databaseVersion = rs.getString(1);
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
	public int getJDBCMajorVersion() throws SQLException {
		return VersionUtil.extractMajorVersion(VersionUtil.getSpecificationVersion());
	}

	@Override
	public int getJDBCMinorVersion() throws SQLException {
		return VersionUtil.extractMinorVersion(VersionUtil.getSpecificationVersion());
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
		if (isWrapperFor(iface)) {
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
		return connection.getSessionProperties().getPrincipal();
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		return false;
	}

	@Override
	public boolean nullsAreSortedHigh() throws SQLException {
		return false;
	}

	@Override
	public boolean nullsAreSortedLow() throws SQLException {
		return !nullsAreSortedHigh();
	}

	@Override
	public boolean nullsAreSortedAtStart() throws SQLException {
		return false;
	}

	@Override
	public boolean nullsAreSortedAtEnd() throws SQLException {
		return !nullsAreSortedAtStart();
	}

	@Override
	public boolean usesLocalFiles() throws SQLException {
		return false;
	}

	@Override
	public boolean usesLocalFilePerTable() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMixedCaseIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesUpperCaseIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesLowerCaseIdentifiers() throws SQLException {
		return true;
	}

	@Override
	public boolean storesMixedCaseIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
		return true;
	}

	@Override
	public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
		return true;
	}

	@Override
	public String getIdentifierQuoteString() throws SQLException {
		return "\"";
	}

	@Override
	public String getSQLKeywords() {
		// Firebolt reserved words minus SQL:2003 keywords
		return "BOOL,CONCAT,COPY,DATABASE,DATETIME,DOUBLECOLON,DOW,"
				+ "DOY,EMPTY_IDENTIFIER,EPOCH,EXPLAIN,EXTRACT,FIRST,GENERATE,ILIKE,ISNULL,"
				+ "JOIN_TYPE,LIMIT,LIMIT_DISTINCT,LONG,NEXT,OFFSET,PRIMARY,QUARTER,SAMPLE,SHOW,TEXT,"
				+ "TOP,TRIM,TRUNCATE,UNKNOWN_CHAR,UNTERMINATED_STRING,WEEK";
	}

	@Override
	public String getNumericFunctions() {
		return "ABS,ACOS,ASIN,ATAN,ATAN2,CBRT,CEIL,CEILING,COS,COT,DEGREES,EXP,FLOOR,LOG,MOD,PI,POW,,POWER,RADIANS,RANDOM,ROUND,SIGN,SIN,SQRT,TAN,TRUNC";
	}

	@Override
	public String getStringFunctions() {
		return "BASE64_ENCODE,CONCAT,EXTRACT_ALL,GEN_RANDOM_UUID,ILIKE,LENGTH,LIKE,LOWER,LPAD,LTRIM,MATCH,MATCH_ANY,"
				+ "MD5,MD5_NUMBER_LOWER64,MD5_NUMBER_UPPER64,REGEXP_LIKE,REGEXP_MATCHES,REGEXP_REPLACE,REPEAT,REPLACE,REVERSE,"
				+ "RPAD,RTRIM,SPLIT,SPLIT_PART,STRPOS,SUBSTRING,TO_DATE,TO_DOUBLE,TO_FLOAT,TO_INT,TO_TIMESTAMP,TO_TIMESTAMPTZ,TRIM,UPPER";
	}

	@Override
	public String getSystemFunctions() {
		return "IFNULL";
	}

	@Override
	public String getTimeDateFunctions() {
		return "CURRENT_DATE,CURRENT_TIMESTAMP,DATE_ADD,DATE_DIFF,DATE_TRUNC,EXTRACT,LOCALTIMESTAMP,TO_CHAR,TO_DATE,TO_TIMESTAMP,TO_TIMESTAMPTZ";
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public String getSearchStringEscape() throws SQLException {
		return "\\";
	}

	/**
	 * Returns empty string for compatibility with PostgreSQL.
	 * @return empty string
	 * @throws SQLException - if fact does not throw exception because the implementation is trivial
	 */
	@Override
	public String getExtraNameCharacters() throws SQLException {
		return "";
	}

	@Override
	public boolean supportsAlterTableWithAddColumn() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsAlterTableWithDropColumn() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsColumnAliasing() throws SQLException {
		return true;
	}

	@Override
	public boolean nullPlusNonNullIsNull() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsConvert() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsConvert(int fromType, int toType) throws SQLException {
		return false;
	}

	@Override
	public boolean supportsTableCorrelationNames() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsDifferentTableCorrelationNames() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsExpressionsInOrderBy() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsOrderByUnrelated() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsGroupBy() throws SQLException {
		return true;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsGroupByUnrelated() throws SQLException {
		return false;
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
	public boolean supportsMultipleTransactions() throws SQLException {
		return false;
	}

	@Override
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
	public boolean supportsOuterJoins() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsFullOuterJoins() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsLimitedOuterJoins() throws SQLException {
		return true;
	}

	@Override
	public String getSchemaTerm() throws SQLException {
		return "schema";
	}

	@Override
	public String getProcedureTerm() throws SQLException {
		return "procedure";
	}

	@Override
	public String getCatalogTerm() throws SQLException {
		return "database";
	}

	@Override
	public boolean isCatalogAtStart() throws SQLException {
		// it is currently not supported but it will be soon
		return false;
	}

	@Override
	public String getCatalogSeparator() throws SQLException {
		return ".";
	}

	@Override
	public boolean supportsSchemasInDataManipulation() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSchemasInProcedureCalls() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSchemasInTableDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSchemasInIndexDefinitions() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsCatalogsInDataManipulation() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInProcedureCalls() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInTableDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
		return false;
	}

	@Override
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
	public boolean supportsUnion() throws SQLException {
		return true;
	}

	@Override
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
	@SuppressWarnings("java:S4144") // identical implementation
	public int getMaxColumnNameLength() throws SQLException {
		return MAX_IDENTIFIER_LENGTH;
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
	public int getMaxColumnsInTable() throws SQLException {
		return 1000;
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
	@SuppressWarnings("java:S4144") // identical implementation
	public int getMaxSchemaNameLength() throws SQLException {
		return MAX_IDENTIFIER_LENGTH;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public int getMaxProcedureNameLength() throws SQLException {
		return 0;
	}

	@Override
	@SuppressWarnings("java:S4144") // identical implementation
	public int getMaxCatalogNameLength() throws SQLException {
		return MAX_IDENTIFIER_LENGTH;
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
	@SuppressWarnings("java:S4144") // identical implementation
	public int getMaxTableNameLength() throws SQLException {
		return MAX_IDENTIFIER_LENGTH;
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
	public int getDefaultTransactionIsolation() throws SQLException {
		return Connection.TRANSACTION_NONE;
	}

	@Override
	public boolean supportsTransactions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
		return false;
	}

	@Override
	public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
		return false;
	}

	@Override
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
	public boolean supportsBatchUpdates() throws SQLException {
		// We support it partially (via FireboltPreparedStatement but not with the
		// 'basic' FireboltStatement )
		return false;
	}

	@Override
	public boolean supportsSavepoints() throws SQLException {
		return false;
	}

	@Override
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
	public boolean supportsResultSetHoldability(int holdability) throws SQLException {
		return holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
	}

	/**
	 * Since Firebolt does not support transactions commit does not affect the existing {@code ResultSet} and therefore
	 * it behaves as if it is held between transaction. Therefore, it returns {@link ResultSet#HOLD_CURSORS_OVER_COMMIT}
	 * @return {@link ResultSet#HOLD_CURSORS_OVER_COMMIT}
	 * @throws SQLException if something is going wrong
	 */
	@Override
	public int getResultSetHoldability() throws SQLException {
		return ResultSet.HOLD_CURSORS_OVER_COMMIT;
	}

	@Override
	public int getSQLStateType() throws SQLException {
		return sqlStateSQL;
	}

	@Override
	public boolean locatorsUpdateCopy() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsStatementPooling() throws SQLException {
		return false;
	}

	@Override
	public RowIdLifetime getRowIdLifetime() throws SQLException {
		return RowIdLifetime.ROWID_UNSUPPORTED;
	}

	@Override
	public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
		return false;
	}

	@Override
	public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
		return false;
	}

	@Override
	public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
		return FireboltResultSet.of(QueryResult.builder()
				.columns(Stream.of(
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
				).map(e -> QueryResult.Column.builder().name(e.getKey()).type(e.getValue()).build()).collect(toList()))
				.rows(List.of())
				.build());
	}

	@Override
	public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
		return FireboltResultSet.of(QueryResult.builder()
				.columns(Stream.of(
						entry(TYPE_CAT, TEXT),
						entry(TYPE_SCHEM, TEXT),
						entry(TYPE_NAME, TEXT),
						entry(CLASS_NAME, TEXT),
						entry(DATA_TYPE, INTEGER),
						entry(REMARKS, TEXT),
						entry(BASE_TYPE, INTEGER) // short
				).map(e -> QueryResult.Column.builder().name(e.getKey()).type(e.getValue()).build()).collect(toList()))
				.rows(List.of())
				.build());
	}

	@Override
	public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
		return FireboltResultSet.of(QueryResult.builder()
				.columns(Stream.of(
						entry(TYPE_CAT, TEXT),
						entry(TYPE_SCHEM, TEXT),
						entry(TYPE_NAME, TEXT),
						entry(SUPERTYPE_CAT, TEXT),
						entry(SUPERTYPE_SCHEM, TEXT),
						entry(SUPERTYPE_NAME, TEXT)
				).map(e -> QueryResult.Column.builder().name(e.getKey()).type(e.getValue()).build()).collect(toList()))
				.rows(List.of())
				.build());
	}

	@Override
	public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
		return FireboltResultSet.of(QueryResult.builder()
				.columns(Stream.of(
						entry(TYPE_CAT, TEXT),
						entry(TYPE_SCHEM, TEXT),
						entry(TYPE_NAME, TEXT),
						entry(SUPERTABLE_NAME, TEXT)
				).map(e -> QueryResult.Column.builder().name(e.getKey()).type(e.getValue()).build()).collect(toList()))
				.rows(List.of())
				.build());
	}

	@Override
	public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
		return FireboltResultSet.of(QueryResult.builder()
				.columns(Stream.of(
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
				).map(e -> QueryResult.Column.builder().name(e.getKey()).type(e.getValue()).build()).collect(toList()))
				.rows(List.of())
				.build());
	}

	@Override
	public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
		return FireboltResultSet.of(QueryResult.builder()
				.columns(Stream.of(
						entry(PROCEDURE_CAT, TEXT),
						entry(PROCEDURE_SCHEM, TEXT),
						entry(PROCEDURE_NAME, TEXT),
						entry(REMARKS, TEXT),
						entry(PROCEDURE_TYPE, INTEGER), // short
						entry(SPECIFIC_NAME, TEXT)
				).map(e -> QueryResult.Column.builder().name(e.getKey()).type(e.getValue()).build()).collect(toList()))
				.rows(List.of())
				.build());
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
		return FireboltResultSet.empty();
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
		return FireboltResultSet.empty();
	}

	@Override
	public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
		return FireboltResultSet.of(QueryResult.builder()
				.columns(Stream.of(
						entry(SCOPE, INTEGER), // short
						entry(COLUMN_NAME, TEXT),
						entry(DATA_TYPE, INTEGER),
						entry(TYPE_NAME, TEXT),
						entry(COLUMN_SIZE, INTEGER),
						entry(BUFFER_LENGTH, INTEGER),
						entry(DECIMAL_DIGITS, INTEGER), // short
						entry(PSEUDO_COLUMN, INTEGER) // short
				).map(e -> QueryResult.Column.builder().name(e.getKey()).type(e.getValue()).build()).collect(toList()))
				.rows(List.of())
				.build());
	}

	@Override
	public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
		return FireboltResultSet.of(QueryResult.builder()
				.columns(Stream.of(
						entry(SCOPE, INTEGER), // short
						entry(COLUMN_NAME, TEXT),
						entry(DATA_TYPE, INTEGER),
						entry(TYPE_NAME, TEXT),
						entry(COLUMN_SIZE, INTEGER),
						entry(BUFFER_LENGTH, INTEGER),
						entry(DECIMAL_DIGITS, INTEGER), // short
						entry(PSEUDO_COLUMN, INTEGER) // short
				).map(e -> QueryResult.Column.builder().name(e.getKey()).type(e.getValue()).build()).collect(toList()))
				.rows(List.of())
				.build());
	}

	@Override
	public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
		return FireboltResultSet.of(QueryResult.builder()
				.columns(Stream.of(
						entry(TABLE_CAT, TEXT),
						entry(TABLE_SCHEM, TEXT),
						entry(TABLE_NAME, TEXT),
						entry(COLUMN_NAME, TEXT),
						entry(KEY_SEQ, INTEGER), // short
						entry(PK_NAME, TEXT)
				).map(e -> QueryResult.Column.builder().name(e.getKey()).type(e.getValue()).build()).collect(toList()))
				.rows(List.of())
				.build());
	}

	@Override
	public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
		return FireboltResultSet.of(QueryResult.builder()
				.columns(Stream.of(
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
						).map(e -> QueryResult.Column.builder().name(e.getKey()).type(e.getValue()).build()).collect(toList()))
				.rows(List.of())
				.build());
	}

	@Override
	public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
		return FireboltResultSet.of(QueryResult.builder()
				.columns(Stream.of(
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
				).map(e -> QueryResult.Column.builder().name(e.getKey()).type(e.getValue()).build()).collect(toList()))
				.rows(List.of())
				.build());
	}

	@Override
	public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
			String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
		return FireboltResultSet.of(QueryResult.builder()
				.columns(Stream.of(
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
				).map(e -> QueryResult.Column.builder().name(e.getKey()).type(e.getValue()).build()).collect(toList()))
				.rows(List.of())
				.build());
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
		return FireboltResultSet.empty();
	}

	@Override
	public ResultSet getClientInfoProperties() throws SQLException {
		return FireboltResultSet.of(QueryResult.builder()
				.columns(Stream.of(
						entry(NAME, TEXT),
						entry(MAX_LEN, INTEGER),
						entry(DEFAULT_VALUE, TEXT),
						entry(DESCRIPTION, TEXT)
				).map(e -> QueryResult.Column.builder().name(e.getKey()).type(e.getValue()).build()).collect(toList()))
				.rows(List.of())
				.build());
	}

	@Override
	public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
		List<QueryResult.Column> columns = Arrays.asList(
				QueryResult.Column.builder().name(FUNCTION_CAT).type(TEXT).build(),
				QueryResult.Column.builder().name(FUNCTION_SCHEM).type(TEXT).build(),
				QueryResult.Column.builder().name(FUNCTION_NAME).type(TEXT).build(),
				QueryResult.Column.builder().name(REMARKS).type(TEXT).build(),
				QueryResult.Column.builder().name(FUNCTION_TYPE).type(INTEGER).build(),
				QueryResult.Column.builder().name(SPECIFIC_NAME).type(TEXT).build());
		Predicate<String> functionFilter = functionNamePattern == null ? f -> true : compile(functionNamePattern, CASE_INSENSITIVE).asPredicate();

		List<List<?>> rows = Arrays.stream(String.join(",", getStringFunctions(), getNumericFunctions(), getTimeDateFunctions(), getSystemFunctions()).split(","))
				.map(String::trim) // instead of split("\\s*,\\s") blocked by Sonar according to its opinion "can lead denial of service" (?!)
				.filter(functionFilter)
				.sorted()
				.distinct() // some functions belong to different categories, e.g. TO_DATE is both date-time and string function
				.map(function -> Arrays.asList(null, null, function, null, functionNoTable, function))
				.collect(toList());
		return FireboltResultSet.of(QueryResult.builder().columns(columns).rows(rows).build());
	}

	@Override
	public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {

		List<QueryResult.Column> columns = Arrays.asList(
				QueryResult.Column.builder().name(FUNCTION_CAT).type(TEXT).build(),
				QueryResult.Column.builder().name(FUNCTION_SCHEM).type(TEXT).build(),
				QueryResult.Column.builder().name(FUNCTION_NAME).type(TEXT).build(),
				QueryResult.Column.builder().name(COLUMN_NAME).type(TEXT).build(),
				QueryResult.Column.builder().name(COLUMN_TYPE).type(INTEGER).build(),
				QueryResult.Column.builder().name(DATA_TYPE).type(INTEGER).build(),
				QueryResult.Column.builder().name(TYPE_NAME).type(TEXT).build(),
				QueryResult.Column.builder().name(PRECISION).type(INTEGER).build(),
				QueryResult.Column.builder().name(LENGTH).type(INTEGER).build(),
				QueryResult.Column.builder().name(SCALE).type(INTEGER).build(),
				QueryResult.Column.builder().name(RADIX).type(INTEGER).build(),
				QueryResult.Column.builder().name(NULLABLE).type(INTEGER).build(),
				QueryResult.Column.builder().name(REMARKS).type(TEXT).build(),
				QueryResult.Column.builder().name(CHAR_OCTET_LENGTH).type(INTEGER).build(),
				QueryResult.Column.builder().name(ORDINAL_POSITION).type(INTEGER).build(),
				QueryResult.Column.builder().name(IS_NULLABLE).type(TEXT).build(),
				QueryResult.Column.builder().name(SPECIFIC_NAME).type(TEXT).build()
		);
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

		return FireboltResultSet.of(QueryResult.builder().columns(columns).rows(allFunctions).build());
	}
	@Override
	public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
		return FireboltResultSet.of(QueryResult.builder()
				.columns(Stream.of(
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
				).map(e -> QueryResult.Column.builder().name(e.getKey()).type(e.getValue()).build()).collect(toList()))
				.rows(List.of())
				.build());
	}

	@Override
	public boolean generatedKeyAlwaysReturned() throws SQLException {
		return false;
	}
}
