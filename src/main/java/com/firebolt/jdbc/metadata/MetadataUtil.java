package com.firebolt.jdbc.metadata;

import lombok.Builder;
import lombok.Value;
import lombok.experimental.UtilityClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static java.lang.String.format;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.joining;

@UtilityClass
public class MetadataUtil {

	// To uncomment once schemas are supported
//	public String getSchemasQuery(String catalog, String schemaPattern) {
//		Query.QueryBuilder queryBuilder = Query.builder();
//		queryBuilder.select(String.format("table_schema AS %s, table_catalog AS %s", TABLE_SCHEM, TABLE_CATALOG));
//
//		queryBuilder.from("information_schema.databases");
//
//		List<String> conditions = new ArrayList<>();
//		// Optional.ofNullable(catalog)
//		// .ifPresent(c -> conditions.add(String.format("catalog_name = '%s'", c)));
//
//		// Uncomment below once schemas are supported
//		// Optional.ofNullable(schemaPattern)
//		// .ifPresent(pattern -> conditions.add(String.format("schema_name LIKE '%s'",
//		// pattern)));
//
//		return queryBuilder.conditions(conditions).build().toSql();
//	}

	public String getColumnsQuery(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) {
		Query.QueryBuilder queryBuilder = Query.builder().select(
				"table_schema, table_name, column_name, data_type, column_default, is_nullable, ordinal_position")
				.from("information_schema.columns");

		List<String> conditions = new ArrayList<>();
		ofNullable(tableNamePattern).ifPresent(pattern -> conditions.add(format("table_name LIKE '%s'", pattern)));
		ofNullable(columnNamePattern).ifPresent(pattern -> conditions.add(format("column_name LIKE '%s'", pattern)));
		ofNullable(schemaPattern).ifPresent(pattern -> conditions.add(format("table_schema LIKE '%s'", pattern)));
		ofNullable(catalog).ifPresent(pattern -> conditions.add(String.format("table_catalog LIKE '%s'", pattern)));
		return queryBuilder.conditions(conditions).build().toSql();
	}

	public String getTablesQuery(String catalog, String schema, String tableName, String[] types) {
		Query.QueryBuilder queryBuilder = Query.builder().select("table_schema, table_name, table_type").from("information_schema.tables");
		List<String> conditions = new ArrayList<>();
		conditions.add(format("table_type IN (%s)", Arrays.stream(types).map(t -> format("'%s'", t)).collect(joining(", "))));
		ofNullable(catalog).ifPresent(pattern -> conditions.add(String.format("table_catalog LIKE '%s'", pattern)));
		ofNullable(schema).ifPresent(pattern -> conditions.add(format("table_schema LIKE '%s'", pattern)));
		ofNullable(tableName).ifPresent(pattern -> conditions.add(format("table_name LIKE '%s'", pattern)));
		return queryBuilder.conditions(conditions).orderBy("table_schema, table_name").build().toSql();
	}

	/**
	 * Represents a SQL query that can be sent to Firebolt to receive metadata info
	 */
	@Builder
	@Value
	public static class Query {
		String select;
		String from;
		String innerJoin;
		String orderBy;
		List<String> conditions;

		/**
		 * Parse the object to a SQL query that can be sent to Firebolt
		 *
		 * @return SQL query that can be sent to Firebolt
		 */
		public String toSql() {
			StringBuilder query = new StringBuilder();
			if (select == null || select.isBlank()) {
				throw new IllegalStateException("Cannot create query: SELECT cannot be blank");
			}
			if (from == null || from.isBlank()) {
				throw new IllegalStateException("Cannot create query: FROM cannot be blank");
			}

			query.append("SELECT ").append(select);
			query.append(" FROM ").append(from);
			if (innerJoin != null && !innerJoin.isBlank()) {
				query.append(" JOIN ").append(innerJoin);
			}
			query.append(getConditionsPart());
			if (orderBy != null && !orderBy.isBlank()) {
				query.append(" order by ").append(orderBy);
			}
			return query.toString();
		}

		private String getConditionsPart() {
			StringBuilder agg = new StringBuilder();
			Iterator<String> iter = conditions.iterator();
			if (iter.hasNext()) {
				agg.append(" WHERE ");
			}
			if (iter.hasNext()) {
				String entry = iter.next();
				agg.append(entry);
			}
			while (iter.hasNext()) {
				String entry = iter.next();
				agg.append(" AND ").append(entry);
			}
			return agg.toString();
		}
	}

}
