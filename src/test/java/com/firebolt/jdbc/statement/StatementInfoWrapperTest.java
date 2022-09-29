package com.firebolt.jdbc.statement;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;

import org.junit.jupiter.api.Test;

import com.firebolt.jdbc.statement.rawstatement.RawStatement;

class StatementInfoWrapperTest {

	@Test
	void shouldThrowExceptionWhenTryingToInstantiateWithNonEmptyMapOfParamMarkers() {
		RawStatement rawStatement = RawStatement.of("SELECT * FROM EMPLOYEES WHERE name = ?",
				Collections.singletonList(new ParamMarker(1, 145)), "SELECT * FROM EMPLOYEES WHERE name = ?");
		assertThrows(IllegalArgumentException.class, () -> StatementInfoWrapper.of(rawStatement));
	}
}