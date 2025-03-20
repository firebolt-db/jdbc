package com.firebolt.jdbc.connection;

import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.service.FireboltAuthenticationService;
import com.firebolt.jdbc.service.FireboltEngineInformationSchemaService;
import com.firebolt.jdbc.service.FireboltGatewayUrlService;
import com.firebolt.jdbc.service.FireboltStatementService;
import com.firebolt.jdbc.type.ParserVersion;
import java.sql.SQLException;
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


    /**
     * For localhost connection only validate the account that is populated
     *
     * @throws FireboltException
     */
    protected void validateMandatoryConnectionParameters() throws FireboltException {
        String account = loginProperties.getAccount();
        if (StringUtils.isBlank(account)) {
            throw new FireboltException("Cannot connect: account is missing");
        }
    }

}
