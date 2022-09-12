package com.firebolt.jdbc.statement.rawstatement;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class SqlParamMarker {
    int id;
    int position; // Position in the SQL subQuery
}