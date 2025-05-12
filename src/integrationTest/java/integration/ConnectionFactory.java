package integration;

import java.sql.Connection;
import java.sql.SQLException;

public interface ConnectionFactory {

    /**
     * Creates default connection
     */
    Connection create() throws SQLException;

    /**
     * Creates connection with specific options
     * @param connectionOptions - options include database name, or engine name
     * @return
     */
    Connection create(ConnectionOptions connectionOptions) throws SQLException;

    /**
     * The name of the default database
     * @return
     */
    String getDefaultDatabase();

}
