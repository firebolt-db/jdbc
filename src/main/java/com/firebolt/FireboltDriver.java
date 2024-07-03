package com.firebolt;

import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.util.LoggerUtil;
import com.firebolt.jdbc.util.PropertyUtil;
import com.firebolt.jdbc.util.VersionUtil;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

public class FireboltDriver implements Driver {

	public static final String JDBC_FIREBOLT = "jdbc:firebolt:";
	private static final Logger rootLog;
	private static final Logger log;

	static {
		try {
			java.sql.DriverManager.registerDriver(new FireboltDriver());
			rootLog = LoggerUtil.getRootLogger();
			log = Logger.getLogger(FireboltDriver.class.getName());
			log.info("Firebolt Driver registered");
		} catch (SQLException ex) {
			throw new RuntimeException("Cannot register the driver");
		}
	}

	@Override
	public Connection connect(String url, Properties connectionSettings) throws SQLException {
		return acceptsURL(url) ? FireboltConnection.create(url, connectionSettings) : null;
	}

	@Override
	public boolean acceptsURL(String url) {
		return url != null && url.startsWith(JDBC_FIREBOLT);
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
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
	public Logger getParentLogger() {
		return rootLog;
	}
}
