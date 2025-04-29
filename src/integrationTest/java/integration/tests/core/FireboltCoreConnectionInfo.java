package integration.tests.core;

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

    private static final String DEFAULT_HOSTNAME = "localhost";
    private static final Integer DEFAULT_PORT = 3473;
    private static final Map<String, String> DEFAULT_CONNECTION_PARAMS = Map.of(
            "connection_type", "core",
            "ssl", "false",
            "ssl_mode", "none"
    );

    @Builder.Default
    private Optional<String> database = Optional.empty();
    @Builder.Default
    private String host = DEFAULT_HOSTNAME;
    @Builder.Default
    private Integer port = DEFAULT_PORT;
    @Builder.Default
    private Map<String, String> connectionParams = DEFAULT_CONNECTION_PARAMS;

    public String toJdbcUrl() {
        if (host == null || port == null) {
            throw new IllegalStateException("Either host or port are not set");
        }

        String params = connectionParams == null ? "" : connectionParams.entrySet().stream()
                .map(p -> param(p.getKey(), p.getValue()))
                .filter(Objects::nonNull)
                .collect(joining("&"));

        StringBuilder jdbcBuilder = new StringBuilder("jdbc:firebolt://")
                .append(host).append(":").append(port);

        if (database.isPresent()) {
            jdbcBuilder.append("/").append(database.get());
        }

        if (StringUtils.isNotBlank(params)) {
            jdbcBuilder.append("?").append(params);
        }
        return jdbcBuilder.toString();
    }

    private String param(String name, String value) {
        return value == null ? null : format("%s=%s", name, value);
    }
}
