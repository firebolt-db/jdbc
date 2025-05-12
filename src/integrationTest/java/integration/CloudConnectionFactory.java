package integration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * both v1 and v2 use the ConnectionInfo class to create connection so we can use just one factory for both
 */
public class CloudConnectionFactory implements ConnectionFactory {

    @Override
    public Connection create() throws SQLException {
        ConnectionInfo current = integration.ConnectionInfo.getInstance();
        return DriverManager.getConnection(current.toJdbcUrl(),
                integration.ConnectionInfo.getInstance().getPrincipal(),
                integration.ConnectionInfo.getInstance().getSecret());
    }

    @Override
    public Connection create(ConnectionOptions connectionOptions) throws SQLException {
        ConnectionInfo current = integration.ConnectionInfo.getInstance();
        ConnectionInfo updated = new ConnectionInfo(current.getPrincipal(), current.getSecret(),
                current.getEnv(), connectionOptions.getDatabase(), current.getAccount(), connectionOptions.getEngine(), current.getApi(), connectionOptions.getConnectionParams());
        return DriverManager.getConnection(updated.toJdbcUrl(),
                integration.ConnectionInfo.getInstance().getPrincipal(),
                integration.ConnectionInfo.getInstance().getSecret());

    }

    @Override
    public String getDefaultDatabase() {
        return ConnectionInfo.getInstance().getDatabase();
    }
}
