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
import java.util.List;

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
            ",,,,false",
            ",information_schema,,,false",
            ",information%,,,false",
            ",%schema,,,false",
            ",%form%,,,false",
            "{database},,,,false",
            "{database},information_schema,,,false",
            "{database},information%,,,false",
            "{database},%schema,,,false",
            "{database},%form%,,,false",

            ",,%in%,,false",
            ",information_schema,%in%,,false",
            ",information%,%in%,,false",
            ",%schema,%in%,,false",
            ",%form%,%in%,,false",
            "{database},,%in%,,false",
            "{database},information_schema,%in%,,false",
            "{database},information%,%in%,,false",
            "{database},%schema,%in%,,false",
            "{database},%form%,%in%,,false",

            ",,%in%,VIEW,false",
            ",information_schema,%in%,VIEW,false",
            ",information%,%in%,VIEW,false",
            ",%schema,%in%,VIEW,false",
            ",%form%,%in%,VIEW,false",
            "{database},,%in%,VIEW,false",
            "{database},information_schema,%in%,VIEW,false",
            "{database},information%,%in%,VIEW,false",
            "{database},%schema,%in%,VIEW,false",
            "{database},%form%,%in%,VIEW,false",

            ",,,VIEW;TABLE,false",
            ",,%in%,VIEW;TABLE,false",
            ",,,TABLE,true",
    })
    void getTables(String catalog, String schemaPattern, String tableNamePattern, String types, boolean emptyTablesList) throws SQLException {
        String database = integration.ConnectionInfo.getInstance().getDatabase();
        List<List<Object>> rows = readResultSet(dbmd.getTables(catalog, schemaPattern, tableNamePattern, types == null ? null : types.split(";")));
        assertEquals(emptyTablesList, rows.isEmpty());
        for (List<Object> row : rows) {
            assertEquals(database, row.get(0));
            assertEquals("information_schema", row.get(1));
            assertFalse(((String) row.get(2)).isEmpty());
            assertEquals("VIEW", row.get(3));
        }
    }

    @ParameterizedTest
    @CsvSource(value = {
            ",,,,false",
            ",information_schema,,,false",
            ",information%,,,false",
            ",%schema,,,false",
            ",%form%,,,false",
            "{database},,,,false",
            "{database},information_schema,,,false",
            "{database},information%,,,false",
            "{database},%schema,,,false",
            "{database},%form%,,,false",

            ",,%in%,,false",
            ",information_schema,%in%,,false",
            ",information%,%in%,,false",
            ",%schema,%in%,,false",
            ",%form%,%in%,,false",
            "{database},,%in%,,false",
            "{database},information_schema,%in%,,false",
            "{database},information%,%in%,,false",
            "{database},%schema,%in%,,false",
            "{database},%form%,%in%,,false",

            ",,%in%,created,false",
            ",information_schema,%in%,last_altered,false",
            ",information%,%in%,description,false",
            ",%schema,%in%,table_name,false",
            ",%form%,%in%,table_schema,false",
            "{database},,%in%,table_catalog,false",
            "{database},information_schema,%ac%,account_id,false",
            "{database},information%,%in%,name,false",
            "{database},%schema,%in%,region,false",
            "{database},%form%,%in%,type,false",

            ",,,VIEW;nobody,true",
            ",,%in%,VIEW;no-one,true",
            ",,,does-not-exist,true",
    })
    void getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern, boolean emptyTablesList) throws SQLException {
        String database = integration.ConnectionInfo.getInstance().getDatabase();
        List<List<Object>> rows = readResultSet(dbmd.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern));
        assertEquals(emptyTablesList, rows.isEmpty());
        for (List<Object> row : rows) {
            assertEquals(database, row.get(0));
            assertEquals("information_schema", row.get(1));
            assertFalse(((String) row.get(2)).isEmpty());
            assertFalse(((String) row.get(3)).isEmpty());
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
