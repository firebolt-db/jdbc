package com.firebolt.jdbc.connection;

import com.firebolt.jdbc.service.FireboltAuthenticationService;
import com.firebolt.jdbc.service.FireboltEngineInformationSchemaService;
import com.firebolt.jdbc.service.FireboltGatewayUrlService;
import com.firebolt.jdbc.service.FireboltStatementService;
import com.firebolt.jdbc.type.ParserVersion;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static com.firebolt.jdbc.connection.settings.FireboltSessionProperty.ACCESS_TOKEN;
import static com.firebolt.jdbc.connection.settings.FireboltSessionProperty.HOST;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MockitoExtension.class)
public class LocalhostFireboltConnectionServiceTest  {

    private static final String URL = "jdbc:firebolt:db?env=dev&engine=eng&account=dev";

    @Mock
    protected FireboltAuthenticationService fireboltAuthenticationService;
    @Mock
    protected FireboltGatewayUrlService fireboltGatewayUrlService;

    @Mock
    protected FireboltEngineInformationSchemaService fireboltEngineService;
    @Mock
    protected FireboltStatementService fireboltStatementService;

    @ParameterizedTest
    @CsvSource(value = {
            "localhost,access-token,access-token"})
    void shouldGetConnectionTokenFromProperties(String host, String configuredAccessToken, String expectedAccessToken) throws SQLException {
        Properties propsWithToken = new Properties();
        if (host != null) {
            propsWithToken.setProperty(HOST.getKey(), host);
        }
        if (configuredAccessToken != null) {
            propsWithToken.setProperty(ACCESS_TOKEN.getKey(), configuredAccessToken);
        }
        try (FireboltConnection fireboltConnection = createConnection(URL, propsWithToken)) {
            assertEquals(expectedAccessToken, fireboltConnection.getAccessToken().orElse(null));
            Mockito.verifyNoMoreInteractions(fireboltAuthenticationService);
        }
    }

    protected FireboltConnection createConnection(String url, Properties props) throws SQLException {
        return new LocalhostFireboltConnectionServiceSecret(url, props, fireboltAuthenticationService, fireboltGatewayUrlService,
                fireboltStatementService, fireboltEngineService, ParserVersion.CURRENT);

    }
}
