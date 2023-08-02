package com.firebolt.jdbc.metadata;

import com.firebolt.jdbc.resultset.FireboltResultSet;
import com.firebolt.jdbc.testutils.AssertionUtil;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertTrue;

class FireboltSystemEngineDatabaseMetadataTest {
	private final FireboltSystemEngineDatabaseMetadata fireboltSystemEngineDatabaseMetadata = new FireboltSystemEngineDatabaseMetadata(null, null);

	@Test
	void shouldReturnEmptyResultSetWhenGettingTable() throws SQLException {
		ResultSet resultSet = fireboltSystemEngineDatabaseMetadata.getTables(null, null, null, null);
		AssertionUtil.assertResultSetEquality(FireboltResultSet.empty(), resultSet);
	}

	@Test
	void shouldReturnEmptyResultSetWhenGettingColumns() throws SQLException {
		ResultSet resultSet = fireboltSystemEngineDatabaseMetadata.getColumns(null, null, null, null);
		AssertionUtil.assertResultSetEquality(FireboltResultSet.empty(), resultSet);
	}

	@Test
	void shouldReturnEmptyResultSetWhenGettingSchemas() throws SQLException {
		ResultSet resultSet = fireboltSystemEngineDatabaseMetadata.getSchemas();
		AssertionUtil.assertResultSetEquality(FireboltResultSet.empty(), resultSet);
	}

	@Test
	void isReadOnly() throws SQLException {
		assertTrue(fireboltSystemEngineDatabaseMetadata.isReadOnly());
	}
}