package com.firebolt.jdbc.metadata;

import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.metadata.FireboltDatabaseMetadataResult.Column;
import com.firebolt.jdbc.resultset.FireboltResultSet;
import com.firebolt.jdbc.statement.FireboltStatement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.firebolt.jdbc.metadata.MetadataColumns.*;
import static com.firebolt.jdbc.resultset.type.FireboltDataType.INT_32;
import static com.firebolt.jdbc.resultset.type.FireboltDataType.STRING;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FireboltDatabaseMetadataTest {

  @Mock private FireboltConnection fireboltConnection;

  @Mock private FireboltStatement statement;

  @InjectMocks private FireboltDatabaseMetadata fireboltDatabaseMetadata;

  @BeforeEach
  void init() throws SQLException {
    lenient().when(fireboltConnection.createStatement(any())).thenReturn(statement);
    lenient().when(fireboltConnection.createStatement()).thenReturn(statement);
    lenient()
        .when(fireboltConnection.getSessionProperties())
        .thenReturn(FireboltProperties.builder().build());
    lenient().when(statement.executeQuery(any())).thenReturn(FireboltResultSet.empty());
  }

  @Test
  void shouldReturnTableTypes() throws SQLException {
    ResultSet expectedResultSet =
        FireboltDatabaseMetadataResult.builder()
            .columns(
                Collections.singletonList(Column.builder().name("TABLE_TYPE").type(STRING).build()))
            .rows(Collections.singletonList(Arrays.asList("TABLE", "VIEW", "OTHER")))
            .build()
            .toResultSet();

    ResultSet actualResultSet = fireboltDatabaseMetadata.getTableTypes();

    verifyResultSetEquality(expectedResultSet, actualResultSet);
  }

  @Test
  void shouldReturnCatalogs() throws SQLException {
    ResultSet expectedResultSet =
        FireboltDatabaseMetadataResult.builder()
            .columns(
                Collections.singletonList(Column.builder().name(TABLE_CAT).type(STRING).build()))
            .rows(Collections.singletonList(Collections.singletonList(DEFAULT_CATALOG_VALUE)))
            .build()
            .toResultSet();

    ResultSet actualResultSet = fireboltDatabaseMetadata.getCatalogs();

    verifyResultSetEquality(expectedResultSet, actualResultSet);
  }

  @Test
  void shouldNotReturnAnyProcedureColumns() throws SQLException {
    verifyResultSetEquality(
        FireboltResultSet.empty(),
        fireboltDatabaseMetadata.getProcedureColumns(null, null, null, null));
  }

  @Test
  void shouldReturnDatabaseProduct() throws SQLException {
    assertEquals("Firebolt", fireboltDatabaseMetadata.getDatabaseProductName());
  }

  @Test
  void shouldReturnDriverName() throws SQLException {
    assertEquals("Firebolt JDBC Driver", fireboltDatabaseMetadata.getDriverName());
  }

  @Test
  void shouldReturnTrueWhenCorrectTransactionIsolationLevelIsSpecified() throws SQLException {
    assertTrue(fireboltDatabaseMetadata.supportsTransactionIsolationLevel(0));
  }

  @Test
  void shouldReturnFalseWhenIncorrectTransactionIsolationLevelIsSpecified() throws SQLException {
    assertFalse(fireboltDatabaseMetadata.supportsTransactionIsolationLevel(1));
  }

  @Test
  void shouldGetSchemas() throws SQLException {
    String expectedSql =
        "SELECT null AS TABLE_SCHEM, catalog_name AS TABLE_CATALOG FROM information_schema.databases";
    when(statement.executeQuery(expectedSql))
        .thenReturn(new FireboltResultSet(getInputStreamForGetSchemas()));
    ResultSet resultSet = fireboltDatabaseMetadata.getSchemas();
    verify(statement).executeQuery(expectedSql);

    ResultSet expectedResultSet =
        FireboltDatabaseMetadataResult.builder()
            .columns(
                Arrays.asList(
                    Column.builder().name(TABLE_SCHEM).type(STRING).build(),
                    Column.builder().name(TABLE_CATALOG).type(STRING).build()))
            .rows(
                Arrays.asList(
                    Arrays.asList("Tutorial_11_04", "default"), Arrays.asList("system", "default")))
            .build()
            .toResultSet();

    verifyResultSetEquality(expectedResultSet, resultSet);
  }

  @Test
  void shouldGetColumns() throws SQLException {
    String expectedQuery =
        "SELECT table_catalog, table_schema, table_name, column_name, data_type, column_default, is_nullable, ordinal_position FROM information_schema.columns WHERE table_name LIKE 'c' AND column_name LIKE 'd'";

    ResultSet expectedResultSet =
        FireboltDatabaseMetadataResult.builder()
            .columns(
                Arrays.asList(
                    Column.builder().name(TABLE_CAT).type(STRING).build(),
                    Column.builder().name(TABLE_SCHEM).type(STRING).build(),
                    Column.builder().name(TABLE_NAME).type(STRING).build(),
                    Column.builder().name(COLUMN_NAME).type(STRING).build(),
                    Column.builder().name(DATA_TYPE).type(INT_32).build(),
                    Column.builder().name(TYPE_NAME).type(STRING).build(),
                    Column.builder().name(COLUMN_SIZE).type(INT_32).build(),
                    Column.builder().name(BUFFER_LENGTH).type(INT_32).build(),
                    Column.builder().name(DECIMAL_DIGITS).type(INT_32).build(),
                    Column.builder().name(NUM_PREC_RADIX).type(INT_32).build(),
                    Column.builder().name(NULLABLE).type(INT_32).build(),
                    Column.builder().name(REMARKS).type(STRING).build(),
                    Column.builder().name(COLUMN_DEF).type(STRING).build(),
                    Column.builder().name(SQL_DATA_TYPE).type(INT_32).build(),
                    Column.builder().name(SQL_DATETIME_SUB).type(INT_32).build(),
                    Column.builder().name(CHAR_OCTET_LENGTH).type(INT_32).build(),
                    Column.builder().name(ORDINAL_POSITION).type(INT_32).build(),
                    Column.builder().name(IS_NULLABLE).type(STRING).build(),
                    Column.builder().name(SCOPE_CATALOG).type(STRING).build(),
                    Column.builder().name(SCOPE_SCHEMA).type(STRING).build(),
                    Column.builder().name(SCOPE_TABLE).type(STRING).build(),
                    Column.builder().name(SOURCE_DATA_TYPE).type(INT_32).build(),
                    Column.builder().name(IS_AUTOINCREMENT).type(STRING).build(),
                    Column.builder().name(IS_GENERATEDCOLUMN).type(STRING).build()))
            .rows(
                Collections.singletonList(
                    Arrays.asList(
                        DEFAULT_CATALOG_VALUE,
                        null, // schema
                        "D2_TIMESTAMP", // table name
                        "id", // column name
                        Types.INTEGER, // sql data type
                        "INTEGER", // shorter type name
                        11, // Precision of INT
                        null, // buffer length (not used, see Javadoc)
                        0,
                        10, // base of a number system / radix
                        0,
                        "id", // description of the column
                        null,
                        null, // SQL_DATA_TYPE - reserved for future use (see javadoc)
                        null, // SQL_DATETIME_SUB - reserved for future use (see javadoc)
                        null, // CHAR_OCTET_LENGTH - The maximum length of binary and character
                        // based
                        // columns (null for others)
                        1, // The ordinal position
                        "NO",
                        null, // "SCOPE_CATALOG - Unused
                        null, // "SCOPE_SCHEMA" - Unused
                        null, // "SCOPE_TABLE" - Unused
                        null, // "SOURCE_DATA_TYPE" - Unused
                        "NO", // IS_AUTOINCREMENT - Not supported
                        "NO")))
            .build()
            .toResultSet();

    when(statement.executeQuery(expectedQuery))
        .thenReturn(new FireboltResultSet(this.getInputStreamForGetColumns()));

    ResultSet resultSet = fireboltDatabaseMetadata.getColumns("a", "b", "c", "d");
    verify(statement).executeQuery(expectedQuery);
    verifyResultSetEquality(expectedResultSet, resultSet);
  }

  @Test
  void shouldGetTypeInfo() throws SQLException {
    ResultSet resultSet = fireboltDatabaseMetadata.getTypeInfo();
    ResultSet expectedTypeInfo = new FireboltResultSet(this.getExpectedTypeInfo());
    verifyResultSetEquality(expectedTypeInfo, resultSet);
  }

  @Test
  void shouldGetTables() throws SQLException {
    String expectedSqlForTables =
        "SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables WHERE table_name LIKE 'tab%' AND table_catalog LIKE 'catalog' AND table_type NOT LIKE 'EXTERNAL' order by table_schema, table_name";

    String expectedSqlForViews =
        "SELECT table_catalog, table_schema, table_name FROM information_schema.views WHERE table_name LIKE 'tab%' AND table_catalog LIKE 'catalog' order by table_schema, table_name";

    when(statement.executeQuery(expectedSqlForTables))
        .thenReturn(new FireboltResultSet(getInputStreamForGetTables()));
    when(statement.executeQuery(expectedSqlForViews)).thenReturn(FireboltResultSet.empty());

    ResultSet resultSet = fireboltDatabaseMetadata.getTables("catalog", "def%", "tab%", null);

    verify(statement).executeQuery(expectedSqlForTables);
    verify(statement).executeQuery(expectedSqlForViews);

    List<List<?>> rows = new ArrayList<>();
    rows.add(
        Arrays.asList(
            "default",
            null,
            "ex_lineitem",
            "TABLE",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null));
    rows.add(
        Arrays.asList(
            "default", null, "test_1", "TABLE", null, null, null, null, null, null, null, null,
            null));

    ResultSet expectedResultSet =
        FireboltDatabaseMetadataResult.builder()
            .columns(
                Arrays.asList(
                    Column.builder().name(TABLE_CAT).type(STRING).build(),
                    Column.builder().name(TABLE_SCHEM).type(STRING).build(),
                    Column.builder().name(TABLE_NAME).type(STRING).build(),
                    Column.builder().name(TABLE_TYPE).type(STRING).build(),
                    Column.builder().name(REMARKS).type(STRING).build(),
                    Column.builder().name(TYPE_CAT).type(STRING).build(),
                    Column.builder().name(TYPE_SCHEM).type(STRING).build(),
                    Column.builder().name(TYPE_NAME).type(STRING).build(),
                    Column.builder().name(SELF_REFERENCING_COL_NAME).type(STRING).build(),
                    Column.builder().name(REF_GENERATION).type(STRING).build()))
            .rows(rows)
            .build()
            .toResultSet();

    verifyResultSetEquality(expectedResultSet, resultSet);
  }

  @Test
  void shouldGetDatabaseProductVersion() throws SQLException {
    Statement statement = mock(FireboltStatement.class);
    when(fireboltConnection.createStatement()).thenReturn(statement);
    when(fireboltConnection.getEngine()).thenReturn("test");
    when(statement.executeQuery("SELECT version FROM information_schema.engines WHERE engine_name iLIKE 'test%'"))
        .thenReturn(new FireboltResultSet(getInputStreamForGetVersion()));
    assertEquals("abcd_xxx_123", fireboltDatabaseMetadata.getDatabaseProductVersion());
  }

  @Test
  void shouldGetDatabaseMajorVersion() throws SQLException {
    Statement statement = mock(FireboltStatement.class);
    when(fireboltConnection.createStatement()).thenReturn(statement);
    when(fireboltConnection.getEngine()).thenReturn("test");
    when(statement.executeQuery("SELECT version FROM information_schema.engines WHERE engine_name iLIKE 'test%'"))
        .thenReturn(new FireboltResultSet(getInputStreamForGetVersion()));
    assertEquals(0, fireboltDatabaseMetadata.getDatabaseMajorVersion());
  }

  @Test
  void shouldGetDatabaseMinorVersion() throws SQLException {
    Statement statement = mock(FireboltStatement.class);
    when(fireboltConnection.createStatement()).thenReturn(statement);
    when(fireboltConnection.getEngine()).thenReturn("test");
    when(statement.executeQuery("SELECT version FROM information_schema.engines WHERE engine_name iLIKE 'test%'"))
        .thenReturn(new FireboltResultSet(getInputStreamForGetVersion()));
    assertEquals(0, fireboltDatabaseMetadata.getDatabaseMinorVersion());
  }


  private void verifyResultSetEquality(ResultSet expected, ResultSet actual) throws SQLException {
    assertEquals(expected.getMetaData(), actual.getMetaData());
    while (expected.next()) {
      actual.next();
      for (int i = 0; i < expected.getMetaData().getColumnCount(); i++) {
        assertEquals(expected.getObject(i + 1), actual.getObject(i + 1));
      }
    }
  }

  private InputStream getInputStreamForGetColumns() {
    return FireboltDatabaseMetadata.class.getResourceAsStream(
        "/responses/metadata/firebolt-response-get-columns-example");
  }

  private InputStream getInputStreamForGetTables() {
    return FireboltDatabaseMetadata.class.getResourceAsStream(
        "/responses/metadata/firebolt-response-get-tables-example");
  }

  private InputStream getInputStreamForGetSchemas() {
    return FireboltDatabaseMetadata.class.getResourceAsStream(
        "/responses/metadata/firebolt-response-get-schemas-example");
  }

  private InputStream getInputStreamForGetVersion() {
    return FireboltDatabaseMetadata.class.getResourceAsStream(
        "/responses/metadata/firebolt-response-get-version-example");
  }


  private InputStream getExpectedTypeInfo() {
    return FireboltDatabaseMetadata.class.getResourceAsStream("/responses/metadata/expected-types");
  }
}
