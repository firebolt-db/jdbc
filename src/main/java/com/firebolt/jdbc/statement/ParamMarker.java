package com.firebolt.jdbc.statement;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class ParamMarker {
    int id; // Id / index of the param marker in the SQL statement
    int position; // Position in the SQL subStatement
}