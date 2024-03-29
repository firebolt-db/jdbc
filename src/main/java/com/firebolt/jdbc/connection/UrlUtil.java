package com.firebolt.jdbc.connection;

import com.firebolt.jdbc.connection.settings.FireboltSessionProperty;
import lombok.CustomLog;
import lombok.experimental.UtilityClass;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
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
        String cleanURI = jdbcConnectionString.replace(JDBC_PREFIX, "");
        URI uri = URI.create(cleanURI);
        Properties uriProperties = new Properties();
        String query = uri.getQuery();
        if (query != null && !query.isBlank()) {
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
        Optional.ofNullable(uri.getPath()).map(p -> !p.isEmpty() && p.charAt(p.length() - 1) == '/' ? p.substring(0, p.length() - 1) : p)
                .ifPresent(path -> uriProperties.put(FireboltSessionProperty.PATH.getKey(), path));
        Optional.ofNullable(uri.getHost())
                .ifPresent(host -> uriProperties.put(FireboltSessionProperty.HOST.getKey(), host));
        Optional.of(uri.getPort()).filter(p -> !p.equals(-1))
                .ifPresent(port -> uriProperties.put(FireboltSessionProperty.PORT.getKey(), String.valueOf(port)));
        return uriProperties;
    }

    /**
     * This factory method is similar to {@link URI#create(String)}.
     * The difference is that `URI.host` of {@code http://something} is {@code null} while
     * URL spec {@code http://something/} returns URI with host=something.
     * @param spec – the String to parse as a URL.
     * @return URL instance
     */
    public static URL createUrl(String spec) {
        try {
            return new URL(spec);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }
}
