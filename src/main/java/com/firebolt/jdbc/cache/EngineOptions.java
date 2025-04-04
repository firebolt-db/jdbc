package com.firebolt.jdbc.cache;


import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Class that encapsulates all the parameters/options that we can cache for a particular engine
 */
@Getter
@EqualsAndHashCode
public class EngineOptions {

    /**
     * This is the url of the engine. This is the url that should be used when sending queries to the engine name
     */
    private String engineUrl;

    /**
     * List of parameters. These parameters are parsed from the response header to a "USE engine xxx" statement.
     */
    private List<Pair<String,String>> parameters;

    public EngineOptions(String engineUrl, List<Pair<String, String>> parameters) {
        this.engineUrl = engineUrl;
        this.parameters = parameters;
    }
}
