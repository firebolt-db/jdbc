package com.firebolt.jdbc.statement;

import lombok.AllArgsConstructor;
import lombok.Value;

@AllArgsConstructor
@Value
public class ParamMarker {
	int id; // ID / index of the param marker in the SQL statement
	int position; // Position in the SQL subStatement
}