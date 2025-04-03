package com.firebolt.jdbc.connection;

import com.firebolt.jdbc.annotation.ExcludeFromJacocoGeneratedReport;
import com.firebolt.jdbc.cache.CacheServiceProvider;
import com.firebolt.jdbc.cache.CacheType;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.type.ParserVersion;
import com.firebolt.jdbc.util.PropertyUtil;
import java.sql.SQLException;
import java.util.Properties;
import java.util.regex.Pattern;
import lombok.NonNull;

import static java.lang.String.format;

/**
 * Based on the connection url and the connection properties this class will generate the correct firebolt connection
 */
public class FireboltConnectionProvider {


    private FireboltConnectionProviderWrapper fireboltConnectionProviderWrapper;

    public FireboltConnectionProvider() {
        this(new FireboltConnectionProviderWrapper());
    }

    // visible for testing
    FireboltConnectionProvider(FireboltConnectionProviderWrapper fireboltConnectionProviderWrapper) {
        this.fireboltConnectionProviderWrapper = fireboltConnectionProviderWrapper;
    }

    /**
     * From the url and connection properties, determine the appropriate firebolt connection
     */
    public FireboltConnection create(@NonNull String url, Properties connectionSettings) throws SQLException {
        switch(getUrlVersion(url, connectionSettings)) {
            case 1:
                return fireboltConnectionProviderWrapper.createFireboltConnectionUsernamePassword(url, connectionSettings, ParserVersion.LEGACY);
            case 2:
                return isLocalhostConnection(url, connectionSettings) ?
                        fireboltConnectionProviderWrapper.createLocalhostFireboltConnectionServiceSecret(url, connectionSettings) :
                        fireboltConnectionProviderWrapper.createFireboltConnectionServiceSecret(url, connectionSettings);
            default: throw new IllegalArgumentException(format("Cannot distinguish version from url %s", url));
        }
    }

    private boolean isLocalhostConnection(String jdbcUri, Properties connectionProperties) {
        FireboltProperties fireboltProperties = new FireboltProperties(new Properties[] {UrlUtil.extractProperties(jdbcUri), connectionProperties});
        return PropertyUtil.isLocalDb(fireboltProperties);
    }

    private int getUrlVersion(String url, Properties connectionSettings) {
        Pattern urlWithHost = Pattern.compile("jdbc:firebolt://api\\.\\w+\\.firebolt\\.io");
        if (!urlWithHost.matcher(url).find()) {
            return 2; // new URL format
        }
        // old URL format
        Properties propertiesFromUrl = UrlUtil.extractProperties(url);
        Properties allSettings = PropertyUtil.mergeProperties(propertiesFromUrl, connectionSettings);
        if (allSettings.containsKey("client_id") && allSettings.containsKey("client_secret") && !allSettings.containsKey("user") && !allSettings.containsKey("password")) {
            return 2;
        }
        FireboltProperties props = new FireboltProperties(new Properties[] {propertiesFromUrl, connectionSettings});
        String principal = props.getPrincipal();
        if (props.getAccessToken() != null || (principal != null && principal.contains("@"))) {
            return 1;
        }
        return 2;
    }

    /**
     * This is just a wrapper classes for the connection instance creation so we can test them without requiring an actual firebolt 1.0 or firebolt 2.0 backend.
     */
    @ExcludeFromJacocoGeneratedReport
    static class FireboltConnectionProviderWrapper {

        public FireboltConnectionUserPassword createFireboltConnectionUsernamePassword(String url, Properties connectionSettings, ParserVersion parserVersion) throws SQLException {
            return new FireboltConnectionUserPassword(url, connectionSettings, parserVersion);
        }

        public FireboltConnectionServiceSecret createFireboltConnectionServiceSecret(String url, Properties connectionSettings) throws SQLException {
            CacheServiceProvider cacheServiceProvider = CacheServiceProvider.getInstance();
            // the ON_DISK memory caching will be implemented after
            return new FireboltConnectionServiceSecret(url, connectionSettings, cacheServiceProvider.getCacheService(CacheType.IN_MEMORY));
        }

        public LocalhostFireboltConnection createLocalhostFireboltConnectionServiceSecret(String url, Properties connectionSettings) throws SQLException {
            CacheServiceProvider cacheServiceProvider = CacheServiceProvider.getInstance();
            // only in memory caching for localhost connections
            return new LocalhostFireboltConnection(url, connectionSettings, cacheServiceProvider.getCacheService(CacheType.IN_MEMORY));
        }
    }

}
