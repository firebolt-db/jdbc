package com.firebolt.jdbc.connection;

import com.firebolt.jdbc.connection.settings.FireboltSessionProperty;
import lombok.CustomLog;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.StringUtils;

import java.net.URI;
import java.util.Optional;
import java.util.Properties;

@CustomLog
@UtilityClass
public class UrlUtil {

    public static final String JDBC_PREFIX = "jdbc:firebolt:";

    public static Properties extractProperties(String jdbcUrl) {
        return parseUriQueryPart(jdbcUrl);
    }


    private static Properties parseUriQueryPart(String jdbcConnectionString) {
        String cleanURI = StringUtils.replace(jdbcConnectionString, JDBC_PREFIX, "");
        URI uri = URI.create(cleanURI);
        Properties uriProperties = new Properties();
        String query = uri.getQuery();
        if (StringUtils.isNotBlank(query)) {
            String[] queryKeyValues = query.split("&");
            for (String keyValue : queryKeyValues) {
                String[] keyValueTokens = keyValue.split("=");
                if (keyValueTokens.length == 2) {
                    uriProperties.put(keyValueTokens[0], keyValueTokens[1]);
                } else {
                    log.warn("Cannot parse key-pair: {}", keyValue);
                }
            }
        }
        Optional.ofNullable(uri.getPath()).map(p -> !StringUtils.isEmpty(p) ? StringUtils.removeEnd(p, "/") : p).ifPresent(path -> uriProperties.put(FireboltSessionProperty.PATH.getKey(), path));
        Optional.of(uri.getPort()).filter(p -> !p.equals(-1))
                .ifPresent(port -> uriProperties.put(FireboltSessionProperty.PORT.getKey(), String.valueOf(port)));
        return uriProperties;
    }
}
