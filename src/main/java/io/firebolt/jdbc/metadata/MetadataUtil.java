package io.firebolt.jdbc.metadata;

import io.firebolt.jdbc.Query;
import lombok.NonNull;
import lombok.experimental.UtilityClass;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.firebolt.jdbc.metadata.MetadataColumns.TABLE_CATALOG;
import static io.firebolt.jdbc.metadata.MetadataColumns.TABLE_SCHEM;

@UtilityClass
public class MetadataUtil {

  public String getSchemasQuery(String catalog, String schemaPattern) {
    Query.QueryBuilder queryBuilder = Query.builder();
    queryBuilder.select(
        String.format("null AS %s, catalog_name AS %s", TABLE_SCHEM, TABLE_CATALOG));

    queryBuilder.from("information_schema.databases");

    List<String> conditions = new ArrayList<>();
    Optional.ofNullable(catalog)
        .ifPresent(c -> conditions.add(String.format("catalog_name = '%s'", c)));

//    Uncomment below once schemas are supported
//    Optional.ofNullable(schemaPattern)
//            .ifPresent(pattern -> conditions.add(String.format("schema_name LIKE '%s'", pattern)));

    return queryBuilder.conditions(conditions).build().toSql();
  }

  public String getColumnsQuery(
      String schemaPattern, String tableNamePattern, String columnNamePattern) {
    Query.QueryBuilder queryBuilder =
        Query.builder()
            .select(
                "table_catalog, table_schema, table_name, column_name, data_type, column_default, is_nullable, ordinal_position")
            .from("information_schema.columns");

    List<String> conditions = new ArrayList<>();
    Optional.ofNullable(tableNamePattern)
        .ifPresent(pattern -> conditions.add(String.format("table_name LIKE '%s'", pattern)));
    Optional.ofNullable(columnNamePattern)
        .ifPresent(pattern -> conditions.add(String.format("column_name LIKE '%s'", pattern)));
// Uncomment once schemas are supported
//    Optional.ofNullable(schemaPattern)
//        .ifPresent(pattern -> conditions.add(String.format("table_schema LIKE '%s'", pattern))); Schemas are not supported
    return queryBuilder.conditions(conditions).build().toSql();
  }

  public String getTablesQuery(String catalog, String schema, String tableName) {
    Query.QueryBuilder queryBuilder =
        Query.builder()
            .select("table_catalog, table_schema, table_name, table_type")
            .from("information_schema.tables");

    List<String> conditions = getConditionsForTablesAndViews(catalog, schema, tableName);

    queryBuilder.orderBy("table_schema, table_name");
    return queryBuilder.conditions(conditions).build().toSql();
  }

  public String getViewsQuery(String catalog, String schemaPattern, String tableNamePattern) {

    Query.QueryBuilder queryBuilder =
        Query.builder()
            .select("table_catalog, table_schema, table_name")
            .from("information_schema.views");

    List<String> conditions = getConditionsForTablesAndViews(catalog, schemaPattern, tableNamePattern);

    queryBuilder.orderBy("table_schema, table_name");
    return queryBuilder.conditions(conditions).build().toSql();
  }

  @NonNull
  private List<String> getConditionsForTablesAndViews(String catalog, String schema, String tableName) {
    List<String> conditions = new ArrayList<>();
//    Optional.ofNullable(schema)
//        .ifPresent(pattern -> conditions.add(String.format("table_schema LIKE '%s'", pattern)));

    Optional.ofNullable(tableName)
        .ifPresent(pattern -> conditions.add(String.format("table_name LIKE '%s'", pattern)));

    Optional.ofNullable(catalog)
        .ifPresent(pattern -> conditions.add(String.format("table_catalog LIKE '%s'", pattern)));
    return conditions;
  }
}
