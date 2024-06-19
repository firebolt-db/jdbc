package com.firebolt.jdbc.exception;

import lombok.Getter;
import lombok.ToString;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toUnmodifiableMap;

@Getter
@ToString
public class ServerError {
    private final Query query;
    private final Error[] errors;

    public ServerError(Query query, Error[] errors) {
        this.query = query;
        this.errors = errors;
    }

    public ServerError(JSONObject json) {
        this(fromJson(json.optJSONObject("query"), Query::new), fromJson(json.optJSONArray("errors"), Error::new, Error[]::new));
    }

    private static <T> T[] fromJson(JSONArray jsonArray, Function<JSONObject, T> factory, IntFunction<T[]> arrayFactory) {
        return jsonArray == null ? null : IntStream.range(0, jsonArray.length()).boxed().map(jsonArray::getJSONObject).map(factory).toArray(arrayFactory);
    }

    private static <T> T fromJson(JSONObject json, Function<JSONObject, T> factory) {
        return ofNullable(json).map(factory).orElse(null);
    }

    public String getErrorMessage() {
        return errors == null ?
                null
                :
                Arrays.stream(errors)
                        .filter(Objects::nonNull)
                        .map(e -> Stream.of(e.severity, e.source, e.code, e.name, e.description).filter(Objects::nonNull).map(Object::toString).collect(joining(" ")))
                        .collect(joining("; "));
    }

    public Query getQuery() {
        return query;
    }

    public Error[] getErrors() {
        return errors;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ServerError that = (ServerError) o;
        return Objects.equals(query, that.query) && Objects.deepEquals(errors, that.errors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(query, Arrays.hashCode(errors));
    }

    @Getter
    @ToString
    public static class Query {
        private final String queryId;
        private final String requestId;
        private final String queryLabel;

        public Query(String queryId, String requestId, String queryLabel) {
            this.queryId = queryId;
            this.requestId = requestId;
            this.queryLabel = queryLabel;
        }

        Query(JSONObject json) {
            this(json.optString("query_id", null), json.optString("request_id", null), json.optString("query_label", null));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Query query = (Query) o;
            return Objects.equals(queryId, query.queryId) && Objects.equals(requestId, query.requestId) && Objects.equals(queryLabel, query.queryLabel);
        }

        @Override
        public int hashCode() {
            return Objects.hash(queryId, requestId, queryLabel);
        }
    }

    @Getter
    @ToString
    public static class Error {
        private final String code;
        private final String name;
        private final Severity severity;
        private final Source source;
        private final String description;
        private final String resolution;
        private final String helpLink;
        private final Location location;

        @SuppressWarnings("java:S107") // the price of the immutability
        public Error(String code, String name, Severity severity, Source source, String description, String resolution, String helpLink, Location location) {
            this.code = code;
            this.name = name;
            this.severity = severity;
            this.source = source;
            this.description = description;
            this.resolution = resolution;
            this.helpLink = helpLink;
            this.location = location;
        }

        Error(JSONObject json) {
            this(json.optString("code", null), json.optString("name", null),
                    json.optEnum(Severity.class, "severity"),
                    ofNullable(json.optString("source", null)).map(Source::fromText).orElse(Source.UNKNOWN),
                    json.optString("description", null), json.optString("resolution", null), json.optString("helpLink", null),
                    ofNullable(json.optJSONObject("location", null)).map(Location::new).orElse(null));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Error error = (Error) o;
            return Objects.equals(code, error.code) && Objects.equals(name, error.name) && severity == error.severity && source == error.source && Objects.equals(description, error.description) && Objects.equals(resolution, error.resolution) && Objects.equals(helpLink, error.helpLink) && Objects.equals(location, error.location);
        }

        @Override
        public int hashCode() {
            return Objects.hash(code, name, severity, source, description, resolution, helpLink, location);
        }

        public enum Severity {
            ERROR, WARNING,
        }

        public enum Source {
            SYSTEM_ERROR("System Error"),
            USER_ERROR("User Error"),
            UNKNOWN("Unknown"),
            USER_WARNING("User Warning"),
            SYSTEM_WARNING("System Warning"),
            SYSTEM_SEVIER_WARNING("System Sevier Warning"),
            ;
            private final String text;
            private static final Map<String, Source> textToSource = Arrays.stream(values()).collect(toUnmodifiableMap(e -> e.text, e -> e));

            Source(String text) {
                this.text = text;
            }

            public static Source fromText(String text) {
                return textToSource.get(text);
            }

            @Override
            public String toString() {
                return text;
            }
        }

        @Getter
        @ToString
        public static class Location {
            private final int failingLine;
            private final int startOffset;
            private final int endOffset;

            public Location(int failingLine, int startOffset, int endOffset) {
                this.failingLine = failingLine;
                this.startOffset = startOffset;
                this.endOffset = endOffset;
            }

            Location(JSONObject json) {
                this(json.optInt("failingLine"), json.optInt("startOffset"), json.optInt("endOffset"));
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Location location = (Location) o;
                return failingLine == location.failingLine && startOffset == location.startOffset && endOffset == location.endOffset;
            }

            @Override
            public int hashCode() {
                return Objects.hash(failingLine, startOffset, endOffset);
            }
        }
    }
}
