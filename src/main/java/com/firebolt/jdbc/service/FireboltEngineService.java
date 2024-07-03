package com.firebolt.jdbc.service;

import com.firebolt.jdbc.connection.Engine;
import com.firebolt.jdbc.connection.settings.FireboltProperties;

import java.sql.SQLException;

public interface FireboltEngineService {
    Engine getEngine(FireboltProperties properties) throws SQLException;

    default boolean doesDatabaseExist(String database) throws SQLException {
        return true;
    }

}
