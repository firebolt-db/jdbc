package com.firebolt.jdbc.metadata;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.jupiter.api.Test;

import com.firebolt.jdbc.resultset.FireboltResultSet;
import com.firebolt.jdbc.testutils.AssertionUtil;

class FireboltSystemEngineDatabaseMetadataTest {

	@Test
	void shouldReturnEmptyResultSetWhenGettingTable() throws SQLException {
		FireboltSystemEngineDatabaseMetadata fireboltSystemEngineDatabaseMetadata = new FireboltSystemEngineDatabaseMetadata(
				null, null);
		ResultSet resultSet = fireboltSystemEngineDatabaseMetadata.getTables(null, null, null, null);
		AssertionUtil.assertResultSetEquality(FireboltResultSet.empty(), resultSet);
	}

	@Test
	void shouldReturnEmptyResultSetWhenGettingColumns() throws SQLException {
		FireboltSystemEngineDatabaseMetadata fireboltSystemEngineDatabaseMetadata = new FireboltSystemEngineDatabaseMetadata(
				null, null);
		ResultSet resultSet = fireboltSystemEngineDatabaseMetadata.getColumns(null, null, null, null);
		AssertionUtil.assertResultSetEquality(FireboltResultSet.empty(), resultSet);
	}

	@Test
	void shouldReturnEmptyResultSetWhenGettingSchemas() throws SQLException {
		FireboltSystemEngineDatabaseMetadata fireboltSystemEngineDatabaseMetadata = new FireboltSystemEngineDatabaseMetadata(
				null, null);
		ResultSet resultSet = fireboltSystemEngineDatabaseMetadata.getSchemas();
		AssertionUtil.assertResultSetEquality(FireboltResultSet.empty(), resultSet);
	}
}