package com.firebolt.jdbc.metadata;

import com.firebolt.jdbc.QueryResult;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.resultset.FireboltResultSet;
import com.firebolt.jdbc.statement.FireboltStatement;
import com.firebolt.jdbc.testutils.AssertionUtil;
import com.firebolt.jdbc.type.FireboltDataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Callable;

import static com.firebolt.jdbc.metadata.MetadataColumns.TABLE_CATALOG;
import static com.firebolt.jdbc.metadata.MetadataColumns.TABLE_SCHEM;
import static java.lang.String.format;
import static java.util.regex.Pattern.CASE_INSENSITIVE;
import static java.util.regex.Pattern.compile;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FireboltSystemEngineDatabaseMetadataTest {
	private static final List<QueryResult.Column> schemaColumns = List.of(
			QueryResult.Column.builder().name(TABLE_SCHEM).type(FireboltDataType.TEXT).build(),
			QueryResult.Column.builder().name(TABLE_CATALOG).type(FireboltDataType.TEXT).build());
	private static final String QUERY_TEMPLATE = "SELECT.+FROM information_schema.%ss.+WHERE.+table_schema like '%s'";
	@Mock
	private FireboltConnection fireboltConnection;

	@Mock
	private FireboltStatement statement;

	private DatabaseMetaData fireboltSystemEngineDatabaseMetadata;

	@BeforeEach
	void init() throws SQLException {
		fireboltSystemEngineDatabaseMetadata = new FireboltSystemEngineDatabaseMetadata("jdbc:firebolt:host", fireboltConnection);
		lenient().when(fireboltConnection.createStatement()).thenReturn(statement);
		lenient().when(fireboltConnection.getCatalog()).thenReturn("db_name");
		lenient().when(fireboltConnection.getSessionProperties()).thenReturn(FireboltProperties.builder().database("my-db").principal("the-user").build());
	}

	@Test
	void getSchemas() throws Exception {
		getSchemas(() -> fireboltSystemEngineDatabaseMetadata.getSchemas());
	}

	@ParameterizedTest
	@NullSource
	@ValueSource(strings = {"information_schema", "INFORMATION_SCHEMA", "info%"})
	void getSchemasWithSchemaPattern(String schemaPattern) throws Exception {
		getSchemas(() -> fireboltSystemEngineDatabaseMetadata.getSchemas(null, schemaPattern));
	}

	@Test
	void getSchemasWrongSchemaPattern() throws Exception {
		ResultSet mockedRs = FireboltResultSet.of(QueryResult.builder().columns(schemaColumns).build());
		ResultSet expectedRs = FireboltResultSet.of(QueryResult.builder().columns(schemaColumns).build());
		when(statement.executeQuery(matches(compile(format(QUERY_TEMPLATE, "table", "does_not_exist"), CASE_INSENSITIVE)))).thenReturn(mockedRs);
		AssertionUtil.assertResultSetEquality(expectedRs, fireboltSystemEngineDatabaseMetadata.getSchemas(null, "public"));
	}

	private void getSchemas(Callable<ResultSet> schemasGetter) throws Exception {
		ResultSet mockedRs = FireboltResultSet.of(QueryResult.builder().columns(schemaColumns).rows(List.of(List.of("information_schema", "my-catalog"))).build());
		ResultSet expectedRs = FireboltResultSet.of(QueryResult.builder().columns(schemaColumns).rows(List.of(List.of("information_schema", "my-catalog"))).build());
		when(statement.executeQuery(matches(compile(format(QUERY_TEMPLATE, "table", "information_schema"), CASE_INSENSITIVE)))).thenReturn(mockedRs);
		AssertionUtil.assertResultSetEquality(expectedRs, schemasGetter.call());
	}

	@ParameterizedTest
	@ValueSource(strings = {"information_schema", "INFORMATION_SCHEMA", "info%"})
	void shouldReturnEmptyResultSetWhenGettingTable(String schemaPattern) throws Exception {
		getEntities(() -> fireboltSystemEngineDatabaseMetadata.getTables(null, schemaPattern, null, null), "table", "information_schema");
	}

	@ParameterizedTest
	@ValueSource(strings = {"information_schema", "INFORMATION_SCHEMA", "info%"})
	void shouldReturnEmptyResultSetWhenGettingColumns(String schemaPattern) throws Exception {
		getEntities(() -> fireboltSystemEngineDatabaseMetadata.getColumns(null, schemaPattern, null, null), "column", "information_schema");
	}

	@Test
	void isReadOnly() throws SQLException {
		assertFalse(fireboltSystemEngineDatabaseMetadata.isReadOnly());
	}

	private void getEntities(Callable<ResultSet> getter, String entity, String schemaPattern) throws Exception {
		ResultSet rs = mock(ResultSet.class);
		when(statement.executeQuery(matches(compile(format(QUERY_TEMPLATE, entity, schemaPattern), CASE_INSENSITIVE)))).thenReturn(rs);
		assertNotNull(getter.call());
	}

}