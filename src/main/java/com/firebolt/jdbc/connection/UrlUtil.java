package com.firebolt.jdbc.connection;

import com.firebolt.jdbc.connection.settings.FireboltSessionProperty;
import lombok.experimental.UtilityClass;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.util.stream.Collectors.toMap;

@UtilityClass
public class UrlUtil {

    public static final String JDBC_PREFIX = "jdbc:firebolt:";
    private static final Logger log = Logger.getLogger(UrlUtil.class.getName());

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
                    log.log(Level.WARNING, "Cannot parse key-pair: {0}", keyValue);
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

    @SuppressWarnings("java:S3358") // ternary  operator for null-safe extraction of value
    public static Map<String, String> getQueryParameters(URL url) {
        String query = url.getQuery();
        if (query == null || query.isBlank()) {
            return Map.of();
        }
        return Arrays.stream(query.split("&")).map(String::trim).filter(kv -> !kv.isBlank()).map(kv -> kv.split("=", 2)).filter(kv -> kv.length == 2).collect(toMap(
                kv -> kv[0],
                kv -> kv[1],
                (first, second) -> second, // override duplicate value
                () -> new TreeMap<>(CASE_INSENSITIVE_ORDER))); // the URL parameters are case-insensitive
    }
}
