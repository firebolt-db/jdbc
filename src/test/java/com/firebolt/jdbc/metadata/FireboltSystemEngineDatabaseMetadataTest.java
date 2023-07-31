package com.firebolt.jdbc.metadata;

import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.FireboltConnectionServiceSecret;
import com.firebolt.jdbc.connection.FireboltConnectionUserPassword;
import com.firebolt.jdbc.resultset.FireboltResultSet;
import com.firebolt.jdbc.service.FireboltEngineService;
import com.firebolt.jdbc.testutils.AssertionUtil;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

abstract class FireboltSystemEngineDatabaseMetadataTest {
	private final FireboltSystemEngineDatabaseMetadata fireboltSystemEngineDatabaseMetadata;

	public static class FireboltSystemEngineDatabaseMetadataUserPasswordConnectionTest extends FireboltSystemEngineDatabaseMetadataTest {
		FireboltSystemEngineDatabaseMetadataUserPasswordConnectionTest() throws SQLException {
			super();
		}

		@Override
		protected FireboltConnection createConnection(Properties properties) throws SQLException {
			return new FireboltConnectionUserPassword("url", properties, mock(), mock(), mock());
		}
	}

	public static class FireboltSystemEngineDatabaseMetadataUserClientSecretTest extends FireboltSystemEngineDatabaseMetadataTest {
		FireboltSystemEngineDatabaseMetadataUserClientSecretTest() throws SQLException {
			super();
		}

		@Override
		protected FireboltConnection createConnection(Properties properties) throws SQLException {
			FireboltEngineService fireboltEngineService = mock(FireboltEngineService.class);
			when(fireboltEngineService.doesDatabaseExist(properties.getProperty("database"))).thenReturn(true);
			return new FireboltConnectionServiceSecret("url", properties, mock(), mock(), mock(), fireboltEngineService, mock());
		}
	}

	FireboltSystemEngineDatabaseMetadataTest() throws SQLException {
		Properties properties = new Properties();
		properties.setProperty("host", "localhost");
		properties.setProperty("database", "my-db");
		fireboltSystemEngineDatabaseMetadata = new FireboltSystemEngineDatabaseMetadata(null, createConnection(properties));
	}

	protected abstract FireboltConnection createConnection(Properties properties) throws SQLException;

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