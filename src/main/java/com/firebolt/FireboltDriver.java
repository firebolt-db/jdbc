package com.firebolt;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.commons.lang3.StringUtils;

import com.firebolt.jdbc.PropertyUtil;
import com.firebolt.jdbc.VersionUtil;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.exception.FireboltSQLFeatureNotSupportedException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FireboltDriver implements Driver {

	public static final String JDBC_FIREBOLT = "jdbc:firebolt:";
	private static final String JDBC_FIREBOLT_PREFIX = JDBC_FIREBOLT + "//";

	static {
		try {
			java.sql.DriverManager.registerDriver(new FireboltDriver());
			log.info("Firebolt Driver registered");
		} catch (SQLException ex) {
			throw new RuntimeException("Cannot register the driver");
		}
	}

	@Override
	public Connection connect(String url, Properties connectionSettings) throws SQLException {
		if (!acceptsURL(url)) {
			return null;
		} else {
			return new FireboltConnection(url, connectionSettings);
		}
	}

	@Override
	public boolean acceptsURL(String url) {
		return StringUtils.isNotEmpty(url) && url.startsWith(JDBC_FIREBOLT_PREFIX);
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
		return PropertyUtil.getPropertyInfo(url, info);
	}

	@Override
	public int getMajorVersion() {
		return VersionUtil.getMajorDriverVersion();
	}

	@Override
	public int getMinorVersion() {
		return VersionUtil.getDriverMinorVersion();
	}

	@Override
	public boolean jdbcCompliant() {
		return false;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		throw new FireboltSQLFeatureNotSupportedException();
	}
}
