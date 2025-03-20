package com.firebolt.jdbc.connection;

import java.sql.SQLException;

/**
 * Class that knows to authenticate to Firebolt
 */
public interface Authenticator {

    void authenticate() throws SQLException;

}
