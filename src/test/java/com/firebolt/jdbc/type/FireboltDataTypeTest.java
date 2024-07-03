package com.firebolt.jdbc.type;


import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.sql.JDBCType;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class FireboltDataTypeTest {
    /**
     * This test validates that {@link FireboltDataType#getSqlType()} return standard type defined in {@link java.sql.Types} and {@link JDBCType}.
     * @param type - the type
     */
    @ParameterizedTest
    @EnumSource(FireboltDataType.class)
    void sqlType(FireboltDataType type) {
        // assert here is just to satisfy static code analysis. valueOf() either succeeds or throws IllegalArgumentException
        assertNotNull(JDBCType.valueOf(type.getSqlType()));
    }
}