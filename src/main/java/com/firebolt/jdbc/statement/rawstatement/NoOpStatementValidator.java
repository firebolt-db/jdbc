package com.firebolt.jdbc.statement.rawstatement;

public class NoOpStatementValidator implements StatementValidator {
    @Override
    public void validate(RawStatement statement) {
        // do nothing
    }
}
