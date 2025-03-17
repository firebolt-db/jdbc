package com.firebolt.jdbc.cache;

import java.util.List;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Class that encapsulates all the parameters/options that we can cache for a particular database
 */
@Getter
public class DatabaseOptions {

    /**
     * List of parameters. These parameters are parsed from the response header to a "USE database xxx" statement.
     */
    private List<Pair<String,String>> parameters;

    public DatabaseOptions(List<Pair<String, String>> parameters) {
        this.parameters = parameters;
    }
}
