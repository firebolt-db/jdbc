package com.firebolt.jdbc.service;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class FireboltEngineInformationSchemaServiceUsingCatalogsTableTest extends FireboltEngineInformationSchemaServiceTest{
    FireboltEngineInformationSchemaServiceUsingCatalogsTableTest() {
        super(true);
    }

    @ParameterizedTest
    @CsvSource(value = {"mydb;'';false", "other_db;'database_name,other_db';true"}, delimiter = ';')
    void doesDatabaseExist(String db, String row, boolean expected) throws SQLException {
        PreparedStatement statement = mock(PreparedStatement.class);
        Map<String, String> rowData = row == null || row.isEmpty() ? Map.of() : Map.of(row.split(",")[0], row.split(",")[1]);
        ResultSet resultSet = mockedResultSet(rowData);
        when(fireboltConnection.prepareStatement("SELECT catalog_name FROM information_schema.catalogs WHERE catalog_name=?")).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        assertEquals(expected, fireboltEngineService.doesDatabaseExist(db));
        Mockito.verify(statement, Mockito.times(1)).setString(1, db);
    }

}
