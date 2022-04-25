package io.firebolt.jdbc.connection;

import io.firebolt.jdbc.service.FireboltAuthenticationService;
import io.firebolt.jdbc.service.FireboltEngineService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FireboltConnectionImplTest {

    private final FireboltConnectionTokens fireboltConnectionTokens = FireboltConnectionTokens.builder().build();
    @Mock
    private FireboltAuthenticationService fireboltAuthenticationService;
    @Mock
    private FireboltEngineService fireboltEngineService;

    @Test
    void shouldInitConnection() throws IOException, NoSuchAlgorithmException {
        String uri = "jdbc:firebolt://firebolt.io/db";
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "user");
        connectionProperties.put("password", "pa$$word");

        when(fireboltAuthenticationService.getConnectionTokens("https://firebolt.io", "user", "pa$$word")).thenReturn(fireboltConnectionTokens);
        FireboltConnectionImpl fireboltConnection = new FireboltConnectionImpl(uri, connectionProperties, fireboltAuthenticationService, fireboltEngineService);
        verify(fireboltAuthenticationService).getConnectionTokens("https://firebolt.io", "user", "pa$$word");
        assertNotNull(fireboltConnection);
    }

}