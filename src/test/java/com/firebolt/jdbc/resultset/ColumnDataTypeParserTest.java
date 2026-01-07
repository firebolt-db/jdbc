package com.firebolt.jdbc.resultset;

import com.firebolt.jdbc.resultset.column.Column;
import com.firebolt.jdbc.resultset.column.ColumnType;
import com.firebolt.jdbc.type.FireboltDataType;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ColumnDataTypeParserTest {

    private static final String[] SINGLE_COLUMN_NAME = new String[] {"col1"};
    private static final boolean ALLOWS_NULLABLE = true;
    private static final boolean DOES_NOT_ALLOW_NULLABLE = false;

    private ColumnDataTypeParser columnDataTypeParser = new ColumnDataTypeParser();

    protected static Stream<Arguments> intDataType() {
        return Stream.of(
                Arguments.of("int null", ALLOWS_NULLABLE, "INT NULL"),
                Arguments.of("int", DOES_NOT_ALLOW_NULLABLE, "INT")
        );
    }

    @ParameterizedTest
    @MethodSource("intDataType")
    void canParseIntegerDataTypeWithNullsAllowed(String columnTypes, boolean isNullable, String dataTypeName) {
        List<Column> columns = columnDataTypeParser.getColumns(SINGLE_COLUMN_NAME, columnTypes);
        assertEquals(1, columns.size());

        Column column = columns.get(0);
        assertEquals("col1", column.getColumnName());
        assertEquals(FireboltDataType.INTEGER, column.getType().getDataType());

        ColumnType columnType = column.getType();
        assertEquals(dataTypeName, columnType.getName());
        assertEquals("integer", columnType.getCompactTypeName());
        assertEquals(FireboltDataType.INTEGER, columnType.getDataType());
        assertEquals(0, columnType.getPrecision());
        assertEquals(0, columnType.getScale());
        assertNull(columnType.getTimeZone());
        assertEquals(Collections.emptyList(), columnType.getInnerTypes());
        assertNull(columnType.getArrayBaseColumnType());
        assertEquals(isNullable, columnType.isNullable());
    }

    @Test
    void canParseMultipleColumnsWithNullTypes() {
        String[] columnNames = new String[] {
                "colInt", "colBigInt", "colNumeric", "colReal", "colDouble",
                "colBoolean", "colDate", "colTimestamp", "colTimestamptz",
                "colText", "colBytea", "colArray", "colStruct", "colGeography"
        };
        String columnTypes = String.join("\t", new String[] {
                "int null",
                "long null",
                "decimal(38,9) null",
                "float null",
                "double null",
                "boolean null",
                "date null",
                "timestamp null",
                "timestamptz null",
                "text null",
                "bytea null",
                "array(integer) null",
                "struct(integer, text) null",
                "geography null"
        });

        List<Column> columns = columnDataTypeParser.getColumns(columnNames, columnTypes);
        assertEquals(columnNames.length, columns.size());

        String[] expectedNames = new String[] {
                "INT NULL", "LONG NULL", "DECIMAL(38,9) NULL", "FLOAT NULL", "DOUBLE NULL",
                "BOOLEAN NULL", "DATE NULL", "TIMESTAMP NULL", "TIMESTAMPTZ NULL",
                "TEXT NULL", "BYTEA NULL", "ARRAY(INTEGER) NULL", "STRUCT(INTEGER, TEXT) NULL", "GEOGRAPHY NULL"
        };
        String[] expectedCompactNames = new String[] {
                "integer", "bigint", "numeric", "real", "double precision",
                "boolean", "date", "timestamp", "timestamptz",
                "text", "bytea", "array(integer)", "STRUCT(INTEGER, TEXT) NULL", "geography"
        };
        FireboltDataType[] expectedDataTypes = new FireboltDataType[] {
                FireboltDataType.INTEGER, FireboltDataType.BIG_INT, FireboltDataType.NUMERIC, FireboltDataType.REAL, FireboltDataType.DOUBLE_PRECISION,
                FireboltDataType.BOOLEAN, FireboltDataType.DATE, FireboltDataType.TIMESTAMP, FireboltDataType.TIMESTAMP_WITH_TIMEZONE,
                FireboltDataType.TEXT, FireboltDataType.BYTEA, FireboltDataType.ARRAY, FireboltDataType.STRUCT, FireboltDataType.GEOGRAPHY
        };
        int[] expectedPrecision = new int[] {0, 0, 38, 0, 0, 1, 0, 6, 6, 0, 0, 0, 0, 0};
        int[] expectedScale = new int[]     {0, 0,  9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            assertEquals(columnNames[i], column.getColumnName());

            ColumnType columnType = column.getType();
            assertEquals(expectedNames[i], columnType.getName());
            assertEquals(expectedCompactNames[i], columnType.getCompactTypeName());
            assertEquals(expectedDataTypes[i], columnType.getDataType());
            assertEquals(expectedPrecision[i], columnType.getPrecision());
            assertEquals(expectedScale[i], columnType.getScale());
            assertNull(columnType.getTimeZone());

            if (expectedDataTypes[i] == FireboltDataType.ARRAY) {
                assertEquals(FireboltDataType.INTEGER, columnType.getArrayBaseColumnType().getDataType());
            } else if (expectedDataTypes[i] == FireboltDataType.STRUCT) {
                assertEquals(2, columnType.getInnerTypes().size());
                assertEquals(FireboltDataType.INTEGER, columnType.getInnerTypes().get(0).getDataType());
                assertEquals(FireboltDataType.TEXT, columnType.getInnerTypes().get(1).getDataType());
                assertEquals(FireboltDataType.INTEGER, columnType.getArrayBaseColumnType().getDataType());
            } else {
                assertEquals(Collections.emptyList(), columnType.getInnerTypes());
                assertNull(columnType.getArrayBaseColumnType());
            }
            assertEquals(true, columnType.isNullable());
        }
    }

    protected static Stream<Arguments> arrayDataType() {
        return Stream.of(
                Arguments.of("array(integer) null", ALLOWS_NULLABLE, "ARRAY(INTEGER) NULL"),
                Arguments.of("array(integer)", DOES_NOT_ALLOW_NULLABLE, "ARRAY(INTEGER)")
        );
    }

    @ParameterizedTest
    @MethodSource("arrayDataType")
    void canParseArrayDataType(String columnTypes, boolean isNullable, String dataTypeName) {
        List<Column> columns = columnDataTypeParser.getColumns(SINGLE_COLUMN_NAME, columnTypes);
        assertEquals(1, columns.size());

        Column column = columns.get(0);
        assertEquals("col1", column.getColumnName());
        assertEquals(FireboltDataType.ARRAY, column.getType().getDataType());

        ColumnType columnType = column.getType();
        assertEquals(dataTypeName, columnType.getName());
        assertEquals("array(integer)", columnType.getCompactTypeName());
        assertEquals(FireboltDataType.ARRAY, columnType.getDataType());
        assertEquals(0, columnType.getPrecision());
        assertEquals(0, columnType.getScale());
        assertNull(columnType.getTimeZone());
        assertEquals(FireboltDataType.INTEGER, columnType.getArrayBaseColumnType().getDataType());
        assertEquals(isNullable, columnType.isNullable());
    }

    protected static Stream<Arguments> structDataType() {
        return Stream.of(
                Arguments.of("struct(integer, text) null", ALLOWS_NULLABLE, "STRUCT(INTEGER, TEXT) NULL"),
                Arguments.of("struct(integer, text)", DOES_NOT_ALLOW_NULLABLE, "STRUCT(INTEGER, TEXT)")
        );
    }

    @ParameterizedTest
    @MethodSource("structDataType")
    void canParseStructDataType(String columnTypes, boolean isNullable, String dataTypeName) {
        List<Column> columns = columnDataTypeParser.getColumns(SINGLE_COLUMN_NAME, columnTypes);
        assertEquals(1, columns.size());

        Column column = columns.get(0);
        assertEquals("col1", column.getColumnName());
        assertEquals(FireboltDataType.STRUCT, column.getType().getDataType());

        ColumnType columnType = column.getType();
        assertEquals(dataTypeName, columnType.getName());
        assertEquals(dataTypeName, columnType.getCompactTypeName());
        assertEquals(FireboltDataType.STRUCT, columnType.getDataType());
        assertEquals(0, columnType.getPrecision());
        assertEquals(0, columnType.getScale());
        assertNull(columnType.getTimeZone());
        assertEquals(2, columnType.getInnerTypes().size());
        assertEquals(FireboltDataType.INTEGER, columnType.getInnerTypes().get(0).getDataType());
        assertEquals(FireboltDataType.TEXT, columnType.getInnerTypes().get(1).getDataType());
        assertEquals(FireboltDataType.INTEGER, columnType.getArrayBaseColumnType().getDataType());
        assertEquals(isNullable, columnType.isNullable());
    }

    protected static Stream<Arguments> geographyDataType() {
        return Stream.of(
                Arguments.of("geography null", ALLOWS_NULLABLE, "GEOGRAPHY NULL"),
                Arguments.of("geography", DOES_NOT_ALLOW_NULLABLE, "GEOGRAPHY")
        );
    }

    @ParameterizedTest
    @MethodSource("geographyDataType")
    void canParseGeographyDataType(String columnTypes, boolean isNullable, String dataTypeName) {
        List<Column> columns = columnDataTypeParser.getColumns(SINGLE_COLUMN_NAME, columnTypes);
        assertEquals(1, columns.size());

        Column column = columns.get(0);
        assertEquals("col1", column.getColumnName());
        assertEquals(FireboltDataType.GEOGRAPHY, column.getType().getDataType());

        ColumnType columnType = column.getType();
        assertEquals(dataTypeName, columnType.getName());
        assertEquals("geography", columnType.getCompactTypeName());
        assertEquals(FireboltDataType.GEOGRAPHY, columnType.getDataType());
        assertEquals(0, columnType.getPrecision());
        assertEquals(0, columnType.getScale());
        assertNull(columnType.getTimeZone());
        assertEquals(Collections.emptyList(), columnType.getInnerTypes());
        assertNull(columnType.getArrayBaseColumnType());
        assertEquals(isNullable, columnType.isNullable());
    }

    protected static Stream<Arguments> booleanDataType() {
        return Stream.of(
                Arguments.of("boolean null", ALLOWS_NULLABLE, "BOOLEAN NULL"),
                Arguments.of("boolean", DOES_NOT_ALLOW_NULLABLE, "BOOLEAN")
        );
    }

    @ParameterizedTest
    @MethodSource("booleanDataType")
    void canParseBooleanDataType(String columnTypes, boolean isNullable, String dataTypeName) {
        List<Column> columns = columnDataTypeParser.getColumns(SINGLE_COLUMN_NAME, columnTypes);
        assertEquals(1, columns.size());

        Column column = columns.get(0);
        assertEquals("col1", column.getColumnName());
        assertEquals(FireboltDataType.BOOLEAN, column.getType().getDataType());

        ColumnType columnType = column.getType();
        assertEquals(dataTypeName, columnType.getName());
        assertEquals("boolean", columnType.getCompactTypeName());
        assertEquals(FireboltDataType.BOOLEAN, columnType.getDataType());
        assertEquals(1, columnType.getPrecision());
        assertEquals(0, columnType.getScale());
        assertNull(columnType.getTimeZone());
        assertEquals(Collections.emptyList(), columnType.getInnerTypes());
        assertNull(columnType.getArrayBaseColumnType());
        assertEquals(isNullable, columnType.isNullable());
    }

    protected static Stream<Arguments> dateDataType() {
        return Stream.of(
                Arguments.of("date null", ALLOWS_NULLABLE, "DATE NULL"),
                Arguments.of("date", DOES_NOT_ALLOW_NULLABLE, "DATE")
        );
    }

    @ParameterizedTest
    @MethodSource("dateDataType")
    void canParseDateDataType(String columnTypes, boolean isNullable, String dataTypeName) {
        List<Column> columns = columnDataTypeParser.getColumns(SINGLE_COLUMN_NAME, columnTypes);
        assertEquals(1, columns.size());

        Column column = columns.get(0);
        assertEquals("col1", column.getColumnName());
        assertEquals(FireboltDataType.DATE, column.getType().getDataType());

        ColumnType columnType = column.getType();
        assertEquals(dataTypeName, columnType.getName());
        assertEquals("date", columnType.getCompactTypeName());
        assertEquals(FireboltDataType.DATE, columnType.getDataType());
        assertEquals(0, columnType.getPrecision());
        assertEquals(0, columnType.getScale());
        assertNull(columnType.getTimeZone());
        assertEquals(Collections.emptyList(), columnType.getInnerTypes());
        assertNull(columnType.getArrayBaseColumnType());
        assertEquals(isNullable, columnType.isNullable());
    }

    protected static Stream<Arguments> timestampDataType() {
        return Stream.of(
                Arguments.of("timestamp null", ALLOWS_NULLABLE, "TIMESTAMP NULL"),
                Arguments.of("timestamp", DOES_NOT_ALLOW_NULLABLE, "TIMESTAMP")
        );
    }

    @ParameterizedTest
    @MethodSource("timestampDataType")
    void canParseTimestampDataType(String columnTypes, boolean isNullable, String dataTypeName) {
        List<Column> columns = columnDataTypeParser.getColumns(SINGLE_COLUMN_NAME, columnTypes);
        assertEquals(1, columns.size());

        Column column = columns.get(0);
        assertEquals("col1", column.getColumnName());
        assertEquals(FireboltDataType.TIMESTAMP, column.getType().getDataType());

        ColumnType columnType = column.getType();
        assertEquals(dataTypeName, columnType.getName());
        assertEquals("timestamp", columnType.getCompactTypeName());
        assertEquals(FireboltDataType.TIMESTAMP, columnType.getDataType());
        assertEquals(6, columnType.getPrecision());
        assertEquals(0, columnType.getScale());
        assertNull(columnType.getTimeZone());
        assertEquals(Collections.emptyList(), columnType.getInnerTypes());
        assertNull(columnType.getArrayBaseColumnType());
        assertEquals(isNullable, columnType.isNullable());
    }

    protected static Stream<Arguments> timestamptzDataType() {
        return Stream.of(
                Arguments.of("timestamptz null", ALLOWS_NULLABLE, "TIMESTAMPTZ NULL"),
                Arguments.of("timestamptz", DOES_NOT_ALLOW_NULLABLE, "TIMESTAMPTZ")
        );
    }

    @ParameterizedTest
    @MethodSource("timestamptzDataType")
    void canParseTimestamptzDataType(String columnTypes, boolean isNullable, String dataTypeName) {
        List<Column> columns = columnDataTypeParser.getColumns(SINGLE_COLUMN_NAME, columnTypes);
        assertEquals(1, columns.size());

        Column column = columns.get(0);
        assertEquals("col1", column.getColumnName());
        assertEquals(FireboltDataType.TIMESTAMP_WITH_TIMEZONE, column.getType().getDataType());

        ColumnType columnType = column.getType();
        assertEquals(dataTypeName, columnType.getName());
        assertEquals("timestamptz", columnType.getCompactTypeName());
        assertEquals(FireboltDataType.TIMESTAMP_WITH_TIMEZONE, columnType.getDataType());
        assertEquals(6, columnType.getPrecision());
        assertEquals(0, columnType.getScale());
        assertNull(columnType.getTimeZone());
        assertEquals(Collections.emptyList(), columnType.getInnerTypes());
        assertNull(columnType.getArrayBaseColumnType());
        assertEquals(isNullable, columnType.isNullable());
    }

    protected static Stream<Arguments> bigintDataType() {
        return Stream.of(
                Arguments.of("long null", ALLOWS_NULLABLE, "LONG NULL"),
                Arguments.of("long", DOES_NOT_ALLOW_NULLABLE, "LONG")
        );
    }

    @ParameterizedTest
    @MethodSource("bigintDataType")
    void canParseBigintDataType(String columnTypes, boolean isNullable, String dataTypeName) {
        List<Column> columns = columnDataTypeParser.getColumns(SINGLE_COLUMN_NAME, columnTypes);
        assertEquals(1, columns.size());

        Column column = columns.get(0);
        assertEquals("col1", column.getColumnName());
        assertEquals(FireboltDataType.BIG_INT, column.getType().getDataType());

        ColumnType columnType = column.getType();
        assertEquals(dataTypeName, columnType.getName());
        assertEquals("bigint", columnType.getCompactTypeName());
        assertEquals(FireboltDataType.BIG_INT, columnType.getDataType());
        assertEquals(0, columnType.getPrecision());
        assertEquals(0, columnType.getScale());
        assertNull(columnType.getTimeZone());
        assertEquals(Collections.emptyList(), columnType.getInnerTypes());
        assertNull(columnType.getArrayBaseColumnType());
        assertEquals(isNullable, columnType.isNullable());
    }

    protected static Stream<Arguments> numericDataType() {
        return Stream.of(
                Arguments.of("DECIMAL(38,9) null", ALLOWS_NULLABLE, "DECIMAL(38,9) NULL", 38, 9),
                Arguments.of("DECIMAL(38,9)", DOES_NOT_ALLOW_NULLABLE, "DECIMAL(38,9)", 38, 9),
                Arguments.of("DECIMAL(18,3)", DOES_NOT_ALLOW_NULLABLE, "DECIMAL(18,3)", 18, 3),
                Arguments.of("DECIMAL(35,13)", DOES_NOT_ALLOW_NULLABLE, "DECIMAL(35,13)", 35, 13)
        );
    }

    @ParameterizedTest
    @MethodSource("numericDataType")
    void canParseNumericDataType(String columnTypes, boolean isNullable, String dataTypeName, int precision, int scale) {
        List<Column> columns = columnDataTypeParser.getColumns(SINGLE_COLUMN_NAME, columnTypes);
        assertEquals(1, columns.size());

        Column column = columns.get(0);
        assertEquals("col1", column.getColumnName());
        assertEquals(FireboltDataType.NUMERIC, column.getType().getDataType());

        ColumnType columnType = column.getType();
        assertEquals(dataTypeName, columnType.getName());
        assertEquals("numeric", columnType.getCompactTypeName());
        assertEquals(FireboltDataType.NUMERIC, columnType.getDataType());
        assertEquals(precision, columnType.getPrecision());
        assertEquals(scale, columnType.getScale());
        assertNull(columnType.getTimeZone());
        assertEquals(Collections.emptyList(), columnType.getInnerTypes());
        assertNull(columnType.getArrayBaseColumnType());
        assertEquals(isNullable, columnType.isNullable());
    }

    protected static Stream<Arguments> realDataType() {
        return Stream.of(
                Arguments.of("float null", ALLOWS_NULLABLE, "FLOAT NULL"),
                Arguments.of("float", DOES_NOT_ALLOW_NULLABLE, "FLOAT")
        );
    }

    @ParameterizedTest
    @MethodSource("realDataType")
    void canParseRealDataType(String columnTypes, boolean isNullable, String dataTypeName) {
        List<Column> columns = columnDataTypeParser.getColumns(SINGLE_COLUMN_NAME, columnTypes);
        assertEquals(1, columns.size());

        Column column = columns.get(0);
        assertEquals("col1", column.getColumnName());
        assertEquals(FireboltDataType.REAL, column.getType().getDataType());

        ColumnType columnType = column.getType();
        assertEquals(dataTypeName, columnType.getName());
        assertEquals("real", columnType.getCompactTypeName());
        assertEquals(FireboltDataType.REAL, columnType.getDataType());
        assertEquals(0, columnType.getPrecision());
        assertEquals(0, columnType.getScale());
        assertNull(columnType.getTimeZone());
        assertEquals(Collections.emptyList(), columnType.getInnerTypes());
        assertNull(columnType.getArrayBaseColumnType());
        assertEquals(isNullable, columnType.isNullable());
    }

    protected static Stream<Arguments> doublePrecisionDataType() {
        return Stream.of(
                Arguments.of("double null", ALLOWS_NULLABLE, "DOUBLE NULL"),
                Arguments.of("double", DOES_NOT_ALLOW_NULLABLE, "DOUBLE")
        );
    }

    @ParameterizedTest
    @MethodSource("doublePrecisionDataType")
    void canParseDoublePrecisionDataType(String columnTypes, boolean isNullable, String dataTypeName) {
        List<Column> columns = columnDataTypeParser.getColumns(SINGLE_COLUMN_NAME, columnTypes);
        assertEquals(1, columns.size());

        Column column = columns.get(0);
        assertEquals("col1", column.getColumnName());
        assertEquals(FireboltDataType.DOUBLE_PRECISION, column.getType().getDataType());

        ColumnType columnType = column.getType();
        assertEquals(dataTypeName, columnType.getName());
        assertEquals("double precision", columnType.getCompactTypeName());
        assertEquals(FireboltDataType.DOUBLE_PRECISION, columnType.getDataType());
        assertEquals(0, columnType.getPrecision());
        assertEquals(0, columnType.getScale());
        assertNull(columnType.getTimeZone());
        assertEquals(Collections.emptyList(), columnType.getInnerTypes());
        assertNull(columnType.getArrayBaseColumnType());
        assertEquals(isNullable, columnType.isNullable());
    }

    protected static Stream<Arguments> textDataType() {
        return Stream.of(
                Arguments.of("text null", ALLOWS_NULLABLE, "TEXT NULL"),
                Arguments.of("text", DOES_NOT_ALLOW_NULLABLE, "TEXT")
        );
    }

    @ParameterizedTest
    @MethodSource("textDataType")
    void canParseTextDataType(String columnTypes, boolean isNullable, String dataTypeName) {
        List<Column> columns = columnDataTypeParser.getColumns(SINGLE_COLUMN_NAME, columnTypes);
        assertEquals(1, columns.size());

        Column column = columns.get(0);
        assertEquals("col1", column.getColumnName());
        assertEquals(FireboltDataType.TEXT, column.getType().getDataType());

        ColumnType columnType = column.getType();
        assertEquals(dataTypeName, columnType.getName());
        assertEquals("text", columnType.getCompactTypeName());
        assertEquals(FireboltDataType.TEXT, columnType.getDataType());
        assertEquals(0, columnType.getPrecision());
        assertEquals(0, columnType.getScale());
        assertNull(columnType.getTimeZone());
        assertEquals(Collections.emptyList(), columnType.getInnerTypes());
        assertNull(columnType.getArrayBaseColumnType());
        assertEquals(isNullable, columnType.isNullable());
    }

    protected static Stream<Arguments> byteaDataType() {
        return Stream.of(
                Arguments.of("bytea null", ALLOWS_NULLABLE, "BYTEA NULL"),
                Arguments.of("bytea", DOES_NOT_ALLOW_NULLABLE, "BYTEA")
        );
    }

    @ParameterizedTest
    @MethodSource("byteaDataType")
    void canParseByteaDataType(String columnTypes, boolean isNullable, String dataTypeName) {
        List<Column> columns = columnDataTypeParser.getColumns(SINGLE_COLUMN_NAME, columnTypes);
        assertEquals(1, columns.size());

        Column column = columns.get(0);
        assertEquals("col1", column.getColumnName());
        assertEquals(FireboltDataType.BYTEA, column.getType().getDataType());

        ColumnType columnType = column.getType();
        assertEquals(dataTypeName, columnType.getName());
        assertEquals("bytea", columnType.getCompactTypeName());
        assertEquals(FireboltDataType.BYTEA, columnType.getDataType());
        assertEquals(0, columnType.getPrecision());
        assertEquals(0, columnType.getScale());
        assertNull(columnType.getTimeZone());
        assertEquals(Collections.emptyList(), columnType.getInnerTypes());
        assertNull(columnType.getArrayBaseColumnType());
        assertEquals(isNullable, columnType.isNullable());
    }

    protected static Stream<Arguments> jsonDataType() {
        return Stream.of(
                Arguments.of("json null", ALLOWS_NULLABLE, "JSON NULL"),
                Arguments.of("json", DOES_NOT_ALLOW_NULLABLE, "JSON")
        );
    }

    @ParameterizedTest
    @MethodSource("jsonDataType")
    void canParseJsonDataType(String columnTypes, boolean isNullable, String dataTypeName) {
        List<Column> columns = columnDataTypeParser.getColumns(SINGLE_COLUMN_NAME, columnTypes);
        assertEquals(1, columns.size());

        Column column = columns.get(0);
        assertEquals("col1", column.getColumnName());
        assertEquals(FireboltDataType.JSON, column.getType().getDataType());

        ColumnType columnType = column.getType();
        assertEquals(dataTypeName, columnType.getName());
        assertEquals("json", columnType.getCompactTypeName());
        assertEquals(FireboltDataType.JSON, columnType.getDataType());
        assertEquals(0, columnType.getPrecision());
        assertEquals(0, columnType.getScale());
        assertNull(columnType.getTimeZone());
        assertEquals(Collections.emptyList(), columnType.getInnerTypes());
        assertNull(columnType.getArrayBaseColumnType());
        assertEquals(isNullable, columnType.isNullable());
    }

}
