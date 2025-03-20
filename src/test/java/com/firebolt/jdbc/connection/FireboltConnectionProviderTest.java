package com.firebolt.jdbc.connection;

import com.firebolt.jdbc.type.ParserVersion;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.Assert.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class FireboltConnectionProviderTest {

    @Mock
    private FireboltConnectionProvider.FireboltConnectionProviderWrapper mockFireboltConnectionProviderWrapper;

    @Mock
    private FireboltConnectionUserPassword mockFireboltConnectionUserPassword;

    @Mock
    private FireboltConnectionServiceSecret mockFireboltConnectionServiceSecret;

    @Mock
    private LocalhostFireboltConnectionServiceSecret mockLocalhostFireboltConnectionServiceSecret;

    private FireboltConnectionProvider fireboltConnectionProvider;

    @BeforeEach
    public void setupMethod() {
        fireboltConnectionProvider = new FireboltConnectionProvider(mockFireboltConnectionProviderWrapper);
    }

    static Stream<Arguments> v1JdbcConnection() {
        return Stream.of(
                Arguments.of("jdbc:firebolt://api.app.firebolt.io", asProperties(Map.of("user", "myuser@email.com", "password", "value"))),
                Arguments.of("jdbc:firebolt://api.app.firebolt.io/my_db", asProperties(Map.of("user", "myuser@email.com", "password", "value"))),
                Arguments.of("jdbc:firebolt://api.app.firebolt.io/my_db?account=developer", asProperties(Map.of("user", "myuser@email.com", "password", "value", "account", "my_account")))
        );
    }
    @ParameterizedTest
    @MethodSource("v1JdbcConnection")
    void canDetectV1Connection(String url, Properties connectionProperties) throws SQLException {
        when(mockFireboltConnectionProviderWrapper.createFireboltConnectionUsernamePassword(url, connectionProperties, ParserVersion.LEGACY)).thenReturn(mockFireboltConnectionUserPassword);
        FireboltConnection fireboltConnection = fireboltConnectionProvider.create(url, connectionProperties);

        assertSame(mockFireboltConnectionUserPassword, fireboltConnection);

        verify(mockFireboltConnectionProviderWrapper, never()).createLocalhostFireboltConnectionServiceSecret(anyString(), any(Properties.class), any(ParserVersion.class));
        verify(mockFireboltConnectionProviderWrapper, never()).createFireboltConnectionServiceSecret(anyString(), any(Properties.class), any(ParserVersion.class));
    }

    @Test
    void cannotCreateV1ConnectionWhenConnectingToBackendFails() throws SQLException {
        String validJdbc1Url = "jdbc:firebolt://api.app.firebolt.io";
        Properties validV1Properties = asProperties(Map.of("user", "myuser@email.com", "password", "value"));

        when(mockFireboltConnectionProviderWrapper.createFireboltConnectionUsernamePassword(validJdbc1Url, validV1Properties, ParserVersion.LEGACY)).thenThrow(SQLException.class);
        assertThrows(SQLException.class, () -> fireboltConnectionProvider.create(validJdbc1Url, validV1Properties));
    }

    static Stream<Arguments> v2JdbcConnection() {
        return Stream.of(
                Arguments.of("jdbc:firebolt:my_db", asProperties(Map.of("client_id", "my client", "client_secret", "my_secret"))),
                Arguments.of("jdbc:firebolt:my_db?engine=my_engine", asProperties(Map.of("client_id", "my client", "client_secret", "my_secret")))
        );
    }

    @ParameterizedTest
    @MethodSource("v2JdbcConnection")
    void canDetectV2Connection(String url, Properties connectionProperties) throws SQLException {
        when(mockFireboltConnectionProviderWrapper.createFireboltConnectionServiceSecret(url, connectionProperties, ParserVersion.CURRENT)).thenReturn(mockFireboltConnectionServiceSecret);

        FireboltConnection fireboltConnection = fireboltConnectionProvider.create(url, connectionProperties);
        assertSame(mockFireboltConnectionServiceSecret, fireboltConnection);

        verify(mockFireboltConnectionProviderWrapper, never()).createLocalhostFireboltConnectionServiceSecret(anyString(), any(Properties.class), any(ParserVersion.class));
        verify(mockFireboltConnectionProviderWrapper, never()).createFireboltConnectionUsernamePassword(anyString(), any(Properties.class), any(ParserVersion.class));
    }

    @Test
    void cannotCreateV2ConnectionWhenConnectingToBackendFails() throws SQLException {
        String validJdbc2Url = "jdbc:firebolt:my_db";
        Properties validV2Properties = asProperties(Map.of("client_id", "my client", "client_secret", "my_secret"));

        when(mockFireboltConnectionProviderWrapper.createFireboltConnectionServiceSecret(validJdbc2Url, validV2Properties, ParserVersion.CURRENT)).thenThrow(SQLException.class);
        assertThrows(SQLException.class, () -> fireboltConnectionProvider.create(validJdbc2Url, validV2Properties));
    }

    static Stream<Arguments> v2LocalhostJdbcConnection() {
        return Stream.of(
                Arguments.of("jdbc:firebolt:my_db", asProperties(Map.of("client_id", "my client", "client_secret", "my_secret","host", "localhost"))),
                Arguments.of("jdbc:firebolt:my_db?engine=my_engine", asProperties(Map.of("client_id", "my_client", "client_secret", "my_secret","host", "localhost")))
        );
    }

    @ParameterizedTest
    @MethodSource("v2LocalhostJdbcConnection")
    void canDetectLocalhostV2Connection(String url, Properties connectionProperties) throws SQLException {
        when(mockFireboltConnectionProviderWrapper.createLocalhostFireboltConnectionServiceSecret(url, connectionProperties, ParserVersion.CURRENT)).thenReturn(mockLocalhostFireboltConnectionServiceSecret);
        FireboltConnection fireboltConnection = fireboltConnectionProvider.create(url, connectionProperties);

        assertSame(mockLocalhostFireboltConnectionServiceSecret, fireboltConnection);

        verify(mockFireboltConnectionProviderWrapper, never()).createFireboltConnectionServiceSecret(anyString(), any(Properties.class), any(ParserVersion.class));
        verify(mockFireboltConnectionProviderWrapper, never()).createFireboltConnectionUsernamePassword(anyString(), any(Properties.class), any(ParserVersion.class));
    }

    @Test
    void cannotCreateV2LocalhostConnectionWhenConnectingToBackendFails() throws SQLException {
        String validJdbc2LocalhostUrl = "jdbc:firebolt:my_db";
        Properties validV2LocalhostProperties = asProperties(Map.of("client_id", "my client", "client_secret", "my_secret","host","localhost"));

        when(mockFireboltConnectionProviderWrapper.createLocalhostFireboltConnectionServiceSecret(validJdbc2LocalhostUrl, validV2LocalhostProperties, ParserVersion.CURRENT)).thenThrow(SQLException.class);
        assertThrows(SQLException.class, () -> fireboltConnectionProvider.create(validJdbc2LocalhostUrl, validV2LocalhostProperties));
    }


    private static Properties asProperties(Map<String, String> map) {
        Properties properties = new Properties();

        map.entrySet().stream().forEach(entry -> properties.setProperty(entry.getKey(), entry.getValue()));

        return properties;
    }


}
