package com.firebolt.jdbc.connection;

import com.firebolt.jdbc.FireboltBackendType;
import com.firebolt.jdbc.annotation.ExcludeFromJacocoGeneratedReport;
import com.firebolt.jdbc.cache.CacheService;
import com.firebolt.jdbc.cache.key.CacheKey;
import com.firebolt.jdbc.cache.key.LocalhostCacheKey;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.service.FireboltAuthenticationService;
import com.firebolt.jdbc.service.FireboltEngineVersion2Service;
import com.firebolt.jdbc.service.FireboltGatewayUrlService;
import com.firebolt.jdbc.service.FireboltStatementService;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

/**
 * A Connection to firebolt that is using localhost as the url of the firebolt server. It will talk to a firebolt 2.0 server.
 */
public class LocalhostFireboltConnection extends FireboltConnectionServiceSecret {

    @ExcludeFromJacocoGeneratedReport
    LocalhostFireboltConnection(@NonNull String url, Properties connectionSettings, CacheService cacheService) throws SQLException {
        super(url, connectionSettings, ConnectionIdGenerator.getInstance(), cacheService);
    }

    // visible for testing
    LocalhostFireboltConnection(@NonNull Pair<String, Properties> urlConnectionSettings,
                                FireboltAuthenticationService fireboltAuthenticationService,
                                FireboltGatewayUrlService fireboltGatewayUrlService,
                                FireboltStatementService fireboltStatementService,
                                FireboltEngineVersion2Service fireboltEngineVersion2Service,
                                ConnectionIdGenerator connectionIdGenerator,
                                CacheService cacheService) throws SQLException {
        super(urlConnectionSettings, fireboltAuthenticationService, fireboltGatewayUrlService, fireboltStatementService, fireboltEngineVersion2Service, connectionIdGenerator, cacheService);
    }

    @Override
    protected void authenticate() throws SQLException {
        super.prepareCacheIfNeeded();

        // When running packdb locally, the login properties are the session properties
        sessionProperties = loginProperties;
    }

    /**
     * The access token should always be available on the login properties.
     */
    @Override
    protected Optional<String> getAccessToken(FireboltProperties fireboltProperties) throws SQLException {
        return Optional.of(fireboltProperties.getAccessToken());
    }

    /**
     * For localhost connection validate:
     *   - accessToken is populated
     *
     *
     * @throws FireboltException
     */
    @Override
    protected void validateConnectionParameters() throws FireboltException {
        // access token is needed for the localhost testing
        String accessToken = loginProperties.getAccessToken();
        if (StringUtils.isBlank(accessToken)) {
            throw new FireboltException("Cannot use localhost host connection without an access token");
        }
    }

    @Override
    protected CacheKey getCacheKey() {
        // when we get to this point we know that the access token is present in the login properties
        return new LocalhostCacheKey(loginProperties.getAccessToken());
    }

    @Override
    public FireboltBackendType getBackendType() {
        return FireboltBackendType.DEV;
    }

}
