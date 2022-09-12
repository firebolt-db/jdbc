package com.firebolt.jdbc.statement;

public enum StatementType {
    PARAM_SETTING, // SET
    QUERY, // eg: SELECT, SHOW
    NON_QUERY // eg: INSERT
}
