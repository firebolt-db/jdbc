package com.firebolt.jdbc.exception;

import org.json.JSONObject;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static com.firebolt.jdbc.exception.ServerError.Error;
import static com.firebolt.jdbc.exception.ServerError.Error.Location;
import static com.firebolt.jdbc.exception.ServerError.Error.Severity;
import static com.firebolt.jdbc.exception.ServerError.Error.Source;
import static com.firebolt.jdbc.exception.ServerError.Query;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ServerErrorTest {
    protected static Stream<Arguments> parse() {
        return Stream.of(
                Arguments.of("{}", new ServerError(null, null)),
                Arguments.of("{\"query\": {}, \"errors\": []}", new ServerError(new Query(null, null, null), new Error[0])),
                Arguments.of("{\"query\": {\"query_id\": \"qid\", \"request_id\": \"rid\", \"query_label\": \"ql\"}, \"errors\": []}", new ServerError(new Query("qid", "rid", "ql"), new Error[0])),
                Arguments.of("{\"errors\": [{}]}", new ServerError(null, new Error[] {new Error(null, null, null, Source.UNKNOWN, null, null, null, null)})),
                Arguments.of("{\"errors\": [{\"code\": \"c1\", \"name\": \"name1\", \"severity\": \"ERROR\", \"source\": \"System Error\", \"description\": \"description1\", \"resolution\": \"resolution1\", \"helpLink\": \"http://help1.com\", \"location\": {\"failing_line\": 1, \"start_offset\": 10, \"end_offset\": 100}}]}",
                        new ServerError(null, new Error[] {new Error("c1", "name1", Severity.ERROR, Source.SYSTEM_ERROR, "description1", "resolution1", "http://help1.com", new Location(1, 10, 100))}))
        );
    }

    @ParameterizedTest
    @MethodSource("parse")
    void parse(String json, ServerError expected) {
        assertEquals(expected, new ServerError(new JSONObject(json)));
    }
}
