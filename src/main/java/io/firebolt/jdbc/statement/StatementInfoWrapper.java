package io.firebolt.jdbc.statement;

import lombok.Builder;
import lombok.Data;


@Data
@Builder
public class StatementInfoWrapper {
    String sql;
    String id;
    boolean query;
}