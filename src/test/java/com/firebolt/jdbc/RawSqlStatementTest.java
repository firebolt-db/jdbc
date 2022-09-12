package com.firebolt.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Collections;

import org.junit.jupiter.api.Test;

class RawSqlStatementTest {

	@Test
	void shouldGetSQL() {
		Query query = Query.builder().select("name, address, department, city").from("employees")
				.conditions(Arrays.asList("name LIKE 'HUG%'", "city LIKE 'LONDO%'")).orderBy("name")
				.innerJoin("ON employees.department = departments.id").build();
		assertEquals(
				"SELECT name, address, department, city FROM employees JOIN ON employees.department = departments.id WHERE name LIKE 'HUG%' AND city LIKE 'LONDO%' order by name",
				query.toSql());
	}

	@Test
	void shouldThrowExceptionWhenRequiredWhereArgumentIsNotProvided() {
		Query query = Query.builder().from("employees").conditions(Collections.singletonList("name LIKE \"HUG%\""))
				.orderBy("name").innerJoin("ON employees.department = departments.id").build();

		assertThrows(IllegalStateException.class, query::toSql);
	}

	@Test
	void shouldThrowExceptionWhenRequiredFromArgumentIsNotProvided() {
		Query query = Query.builder().select("name, address, department")
				.conditions(Collections.singletonList("name LIKE \"HUG%\"")).orderBy("name")
				.innerJoin("ON employees.department = departments.id").build();
		assertThrows(IllegalStateException.class, query::toSql);
	}
}
