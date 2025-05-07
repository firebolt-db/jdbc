package integration.tests.core;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import lombok.Builder;
import org.apache.commons.lang3.StringUtils;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

@Builder
public class FireboltCoreConnectionInfo {

    private static final String JDBC_URL_PREFIX = "jdbc:firebolt:";

    static final String HTTP_PROTOCOL = "http";
    static final String HTTPS_PROTOCOL = "https";

    private static final String DEFAULT_HOSTNAME = "localhost";
    private static final Integer DEFAULT_PORT = 3473;
    private static final Map<String, String> DEFAULT_CONNECTION_PARAMS = new HashMap<>();

    @Builder.Default
    private Optional<String> database = Optional.empty();
    @Builder.Default
    private String url = HTTP_PROTOCOL + ":// " + DEFAULT_HOSTNAME + ":" + DEFAULT_PORT;
    @Builder.Default
    private Map<String, String> connectionParams = DEFAULT_CONNECTION_PARAMS;

    public String toJdbcUrl() {
        String queryParams = getQueryParams();
        StringBuilder jdbcBuilder = new StringBuilder("jdbc:firebolt:");

        if (database.isPresent()) {
            jdbcBuilder.append(database.get());
        }

        if (StringUtils.isNotBlank(queryParams)) {
            jdbcBuilder.append("?").append(queryParams);
        }
        return jdbcBuilder.toString();
    }

    private String getQueryParams() {
        if (StringUtils.isNotBlank(url) && connectionParams !=null && !connectionParams.containsKey("url")) {
            connectionParams.put("url", url);
        }

        String params = connectionParams == null ? "" : connectionParams.entrySet().stream()
        .map(p -> param(p.getKey(), p.getValue()))
        .filter(Objects::nonNull)
        .collect(joining("&"));
        return params;
    }

    private String param(String name, String value) {
        return value == null ? null : format("%s=%s", name, value);
    }
}
