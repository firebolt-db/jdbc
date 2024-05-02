package integration.tests;

import com.firebolt.jdbc.CheckedFunction;
import integration.IntegrationTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.stream.IntStream;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SystemEngineDatabaseMetaDataTest extends IntegrationTest {
    private Connection connection;
    private DatabaseMetaData dbmd;

    @BeforeAll
    void connect() throws SQLException {
        connection = createConnection(getSystemEngineName());
        dbmd = connection.getMetaData();
    }

    @AfterAll
    void disconnect() throws SQLException {
        connection.close();
    }

    @Test
    void readOnly() throws SQLException {
        assertFalse(connection.isReadOnly());
        assertFalse(connection.getMetaData().isReadOnly());
    }

    @Test
    void getSchemas() throws SQLException {
        String database = integration.ConnectionInfo.getInstance().getDatabase();
        assertEquals(List.of(List.of("information_schema", database)), getSchemas(DatabaseMetaData::getSchemas));
    }

    @ParameterizedTest
    @CsvSource(value = {
            ",",
            ",information_schema",
            ",information%",
            ",%schema",
            ",%form%",
            "{database},",
            "{database},information_schema",
            "{database},information%",
            "{database},%schema",
            "{database},%form%",
    })
    void getSchemasInformationSchema(String catalog, String schemaPattern) throws SQLException {
        String database = integration.ConnectionInfo.getInstance().getDatabase();
        String cat = catalog == null ? null :  catalog.replace("{database}", database);
        assertEquals(List.of(List.of("information_schema", database)), getSchemas(dbmd -> dbmd.getSchemas(cat, schemaPattern)));
    }

    @ParameterizedTest
    @CsvSource(value = {
            ",,,,tables,",
            ",information_schema,,,tables,",
            ",information%,,,tables,",
            ",%schema,,,tables,",
            ",%form%,,,tables,",
            "{database},,,,tables,",
            "{database},information_schema,,,tables,",
            "{database},information%,,,tables,",
            "{database},%schema,,,tables,",
            "{database},%form%,,,tables,",

            ",,%in%,,engines,tables",
            ",information_schema,%in%,,engines,tables",
            ",information%,%in%,,engines,tables",
            ",%schema,%in%,,engines,tables",
            ",%form%,%in%,,engines,tables",
            "{database},,%in%,,engines,tables",
            "{database},information_schema,%in%,,engines,tables",
            "{database},information%,%in%,,engines,tables",
            "{database},%schema,%in%,,engines,tables",
            "{database},%form%,%in%,,engines,tables",

            ",,%in%,VIEW,engines,tables",
            ",information_schema,%in%,VIEW,engines,tables",
            ",information%,%in%,VIEW,engines,tables",
            ",%schema,%in%,VIEW,engines,tables",
            ",%form%,%in%,VIEW,engines,tables",
            "{database},,%in%,VIEW,engines,tables",
            "{database},information_schema,%in%,VIEW,engines,tables",
            "{database},information%,%in%,VIEW,engines,tables",
            "{database},%schema,%in%,VIEW,engines,tables",
            "{database},%form%,%in%,VIEW,engines,tables",

            ",,,VIEW;TABLE,engines,",
            ",,%in%,VIEW;TABLE,engines,tables",
            ",,,TABLE,,",
    })
    void getTables(String catalog, String schemaPattern, String tableNamePattern, String types, String requiredTableName, String forbiddenTableName) throws SQLException {
        String database = integration.ConnectionInfo.getInstance().getDatabase();
        String requiredCatalog = catalog == null ? null : catalog.replace("{database}", database);
        String[] requiredTypes = types == null ? null : types.split(";");
        List<List<Object>> rows = readResultSet(dbmd.getTables(requiredCatalog, schemaPattern, tableNamePattern, requiredTypes));
        Collection<String> tables = new HashSet<>();
        for (List<Object> row : rows) {
            assertEquals(database, row.get(0));
            assertEquals("information_schema", row.get(1));
            assertFalse(((String) row.get(2)).isEmpty());
            assertEquals("VIEW", row.get(3));
            tables.add((String) row.get(2));
            System.out.println(row.get(2));
        }
        if (requiredTableName == null) {
            assertTrue(tables.isEmpty());
        } else {
            assertTrue(tables.contains(requiredTableName), format("Required table %s is not found", requiredTableName));
        }
        if (forbiddenTableName != null) {
            assertFalse(tables.contains(forbiddenTableName), format("Forbidden table %s is found", forbiddenTableName));
        }
    }

    @ParameterizedTest
    @CsvSource(value = {
            ",,,,information_schema.columns.column_name,",
            ",information_schema,,,information_schema.columns.column_name,",
            ",information%,,,information_schema.columns.column_name,",
            ",%schema,,,information_schema.columns.column_name,",
            ",%form%,,,information_schema.columns.column_name,",
            "{database},,,,information_schema.columns.column_name,",
            "{database},information_schema,,,information_schema.columns.column_name,",
            "{database},information%,,,information_schema.columns.column_name,",
            "{database},%schema,,,information_schema.columns.column_name,",
            "{database},%form%,,,information_schema.columns.column_name,",

            ",,%in%,,information_schema.engines.engine_name,information_schema.tables.table_name",
            ",information_schema,%in%,,information_schema.engines.engine_name,information_schema.tables.table_name",
            ",information%,%in%,,information_schema.engines.engine_name,information_schema.tables.table_name",
            ",%schema,%in%,,information_schema.engines.engine_name,information_schema.tables.table_name",
            ",%form%,%in%,,information_schema.engines.engine_name,information_schema.tables.table_name",
            "{database},,%in%,,information_schema.engines.engine_name,information_schema.tables.table_name",
            "{database},information_schema,%in%,,information_schema.engines.engine_name,information_schema.tables.table_name",
            "{database},information%,%in%,,information_schema.engines.engine_name,information_schema.tables.table_name",
            "{database},%schema,%in%,,information_schema.engines.engine_name,information_schema.tables.table_name",
            "{database},%form%,%in%,,information_schema.engines.engine_name,information_schema.tables.table_name",

            ",,%in%,created,information_schema.engines.created,information_schema.engines.engine_name",
            ",information_schema,%in%,last_altered,information_schema.engines.last_altered,information_schema.engines.engine_name",
            ",information%,%in%,description,information_schema.engines.description,information_schema.engines.engine_name",
            ",%schema,%in%,table_name,information_schema.indexes.table_name,information_schema.indexes.table_type",
            ",%form%,%in%,table_schema,information_schema.indexes.table_schema,information_schema.indexes.table_name",
            "{database},,%in%,table_catalog,information_schema.indexes.table_catalog,information_schema.indexes.table_schema",
            "{database},information_schema,%ac%,account_id,information_schema.accounts.account_id,information_schema.accounts.region",
            "{database},information%,%in%,engine_name,information_schema.engines.engine_name,information_schema.engines.description",
            "{database},%schema,%in%,region,information_schema.engines.region,information_schema.engines.status",
            "{database},%form%,%in%,type,information_schema.engines.type,information_schema.engines.status",

            ",,,nobody,,information_schema.columns.column_name",
            ",,%in%,no-one,,information_schema.columns.column_name",
            ",,,does-not-exist,,information_schema.columns.column_name",
    })
    void getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern, String requiredColumn, String forbiddenColumn) throws SQLException {
        String database = integration.ConnectionInfo.getInstance().getDatabase();
        String requiredCatalog = catalog == null ? null : catalog.replace("{database}", database);
        List<List<Object>> rows = readResultSet(dbmd.getColumns(requiredCatalog, schemaPattern, tableNamePattern, columnNamePattern));
        Collection<String> columns = new HashSet<>();
        for (List<Object> row : rows) {
            assertEquals(database, row.get(0));
            assertEquals("information_schema", row.get(1));
            assertFalse(((String) row.get(2)).isEmpty());
            assertFalse(((String) row.get(3)).isEmpty());
            columns.add(IntStream.of(1, 2, 3).boxed().map(i -> (String)row.get(i)).collect(joining(".")));
        }
        if (requiredColumn == null) {
            assertTrue(columns.isEmpty());
        } else {
            assertTrue(columns.contains(requiredColumn), format("Required column %s is not found", requiredColumn));
        }
        if (forbiddenColumn != null) {
            assertFalse(columns.contains(forbiddenColumn), format("Forbidden column %s is found", forbiddenColumn));
        }
    }

    private List<List<Object>> getSchemas(CheckedFunction<DatabaseMetaData, ResultSet> schemasGetter) throws SQLException {
        return readResultSet(schemasGetter.apply(dbmd));
    }

    private List<List<Object>> readResultSet(ResultSet rs) throws SQLException {
        List<List<Object>> rows = new ArrayList<>();
        ResultSetMetaData rsmd = rs.getMetaData();
        int n = rsmd.getColumnCount();

        while (rs.next()) {
            List<Object> row = new ArrayList<>();
            rows.add(row);
            for (int i = 1; i <= n; i++) {
                row.add(rs.getObject(i));
            }
        }
        return rows;
    }
}
