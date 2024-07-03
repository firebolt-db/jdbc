package com.firebolt.jdbc.statement.rawstatement;

import com.firebolt.jdbc.connection.FireboltConnection;

public abstract class StatementValidatorFactory {
    private StatementValidatorFactory() {
        // empty private constructor to ensure that this class will be used as factory only.
    }

    public static StatementValidator createValidator(RawStatement statement, FireboltConnection connection) {
        return statement instanceof SetParamRawStatement ? new SetValidator(connection) : new NoOpStatementValidator();
    }
}
