package integration;

import integration.tests.core.FireboltCoreConnectionInfo;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Optional;

public class CoreConnectionFactory implements ConnectionFactory {

    private static final String DATABASE = "integration_test_db";
    private static final String URL = "http://localhost:3473";

    @Override
    public Connection create() throws SQLException {
        String connectionUrl = FireboltCoreConnectionInfo.builder()
                .url(URL)
                .database(Optional.ofNullable(DATABASE))
                .connectionParams(new HashMap<>())
                .build().toJdbcUrl();
        return DriverManager.getConnection(connectionUrl);
    }

    @Override
    public Connection create(ConnectionOptions connectionOptions) throws SQLException {
        String connectionUrl = FireboltCoreConnectionInfo.builder()
                .url(connectionOptions.getUrl())
                .database(Optional.ofNullable(connectionOptions.getDatabase()))
                .connectionParams(connectionOptions.getConnectionParams())
                .build().toJdbcUrl();
        return DriverManager.getConnection(connectionUrl);
    }

    @Override
    public String getDefaultDatabase() {
        // if the database is passed in as parameter use that one, if not use the default one: integration_test_db
        return System.getProperty("database", DATABASE);
    }
}
