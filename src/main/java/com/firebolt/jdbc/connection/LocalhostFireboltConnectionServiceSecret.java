package com.firebolt.jdbc.connection;

import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.service.FireboltAuthenticationService;
import com.firebolt.jdbc.service.FireboltEngineInformationSchemaService;
import com.firebolt.jdbc.service.FireboltGatewayUrlService;
import com.firebolt.jdbc.service.FireboltStatementService;
import com.firebolt.jdbc.type.ParserVersion;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;

/**
 * A Connection to firebolt that is using localhost as the url of the firebolt server. It will talk to a firebolt 2.0 server.
 */
public class LocalhostFireboltConnectionServiceSecret extends FireboltConnectionServiceSecret {

    LocalhostFireboltConnectionServiceSecret(@NonNull String url, Properties connectionSettings, ParserVersion parserVersion) throws SQLException {
        super(url, connectionSettings, parserVersion);
    }

    // visible for testing
    LocalhostFireboltConnectionServiceSecret(@NonNull String url,
                                    Properties connectionSettings,
                                    FireboltAuthenticationService fireboltAuthenticationService,
                                    FireboltGatewayUrlService fireboltGatewayUrlService,
                                    FireboltStatementService fireboltStatementService,
                                    FireboltEngineInformationSchemaService fireboltEngineService,
                                    ParserVersion parserVersion) throws SQLException {
        super(url, connectionSettings, fireboltAuthenticationService, fireboltGatewayUrlService, fireboltStatementService, fireboltEngineService, parserVersion);
    }

    @Override
    protected void authenticate() throws SQLException {
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
     *   - account is populated
     *   - accessToken is populated
     *
     *
     * @throws FireboltException
     */
    @Override
    protected void validateConnectionParameters() throws FireboltException {
        String account = loginProperties.getAccount();
        if (StringUtils.isBlank(account)) {
            throw new FireboltException("Cannot connect: account is missing");
        }

        // access token is needed for the localhost testing
        String accessToken = loginProperties.getAccessToken();
        if (StringUtils.isBlank(accessToken)) {
            throw new FireboltException("Cannot use localhost host connection without an access token");
        }
    }

}
