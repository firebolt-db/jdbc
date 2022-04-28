package io.firebolt.jdbc.client.authentication;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.firebolt.jdbc.ProjectVersionUtil;
import io.firebolt.jdbc.client.authentication.response.FireboltAuthenticationResponse;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.HttpVersion;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicStatusLine;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;


@ExtendWith(MockitoExtension.class)
class FireboltAuthenticationClientTest {
    private final static String HOST = "https://host";
    private final static String USER = "usr";
    private final static String PASSWORD = "PA§§WORD";
    private static MockedStatic<ProjectVersionUtil> mockedProjectVersionUtil;
    @Spy
    private final ObjectMapper objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    @Captor
    ArgumentCaptor<HttpPost> httpPostArgumentCaptor;
    @Mock
    private CloseableHttpClient httpClient;
    private FireboltAuthenticationClient fireboltAuthenticationClient;

    @BeforeAll
    static void init() {
        mockedProjectVersionUtil = mockStatic(ProjectVersionUtil.class);
        mockedProjectVersionUtil.when(ProjectVersionUtil::getProjectVersion).thenReturn("1.0-TEST");
    }

    @AfterAll
    public static void close() {
        mockedProjectVersionUtil.close();
    }

    @BeforeEach
    void setUp() {
        fireboltAuthenticationClient = new FireboltAuthenticationClient(httpClient, objectMapper);
    }

    @Test
    void shouldPostConnectionTokens() throws IOException {
        CloseableHttpResponse response = mock(CloseableHttpResponse.class);
        HttpEntity entity = mock(HttpEntity.class);
        when(response.getEntity()).thenReturn(entity);
        when(response.getStatusLine()).thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, HttpStatus.SC_OK, "OK"));
        when(httpClient.execute(any())).thenReturn(response);
        InputStream tokensResponse = new ByteArrayInputStream(new ObjectMapper().writeValueAsBytes(FireboltAuthenticationResponse.builder().accessToken("a").refreshToken("r").expiresIn(1).build()));
        when(entity.getContent()).thenReturn(tokensResponse);

        fireboltAuthenticationClient.postConnectionTokens(HOST, USER, PASSWORD);

        verify(httpClient).execute(httpPostArgumentCaptor.capture());
        HttpPost actualPost = httpPostArgumentCaptor.getValue();
        assertEquals("User-Agent", actualPost.getAllHeaders()[0].getName());
        assertEquals("fireboltJdbcDriver/1.0-TEST", actualPost.getAllHeaders()[0].getValue());
        verify(objectMapper).readValue("{\"access_token\":\"a\",\"refresh_token\":\"r\",\"expires_in\":1}", FireboltAuthenticationResponse.class);
    }

    @Test
    void shouldThrowExceptionWhenStatusCodeIsNotFound() throws Exception {
        CloseableHttpResponse response = mock(CloseableHttpResponse.class);
        when(response.getStatusLine()).thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, HttpStatus.SC_NOT_FOUND, "NOT FOUND"));
        HttpEntity entity = mock(HttpEntity.class);
        when(response.getEntity()).thenReturn(entity);
        when(httpClient.execute(any())).thenReturn(response);

        assertThrows(IOException.class, () -> fireboltAuthenticationClient.postConnectionTokens(HOST, USER, PASSWORD));

    }

    @Test
    void shouldThrowExceptionWhenStatusCodeIsNotOk() throws Exception {
        CloseableHttpResponse response = mock(CloseableHttpResponse.class);
        when(response.getStatusLine()).thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, HttpStatus.SC_BAD_GATEWAY, "Bad Gateway"));
        HttpEntity entity = mock(HttpEntity.class);
        when(response.getEntity()).thenReturn(entity);
        when(httpClient.execute(any())).thenReturn(response);

        assertThrows(IOException.class, () -> fireboltAuthenticationClient.postConnectionTokens(HOST, USER, PASSWORD));

    }
    @Test
    void shouldThrowExceptionWhenStatusCodeIsForbidden() throws Exception {
        CloseableHttpResponse response = mock(CloseableHttpResponse.class);
        when(response.getStatusLine()).thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, HttpStatus.SC_FORBIDDEN, "FORBIDDEN"));
        HttpEntity entity = mock(HttpEntity.class);
        when(response.getEntity()).thenReturn(entity);
        when(httpClient.execute(any())).thenReturn(response);

        assertThrows(IOException.class, () -> fireboltAuthenticationClient.postConnectionTokens(HOST, USER, PASSWORD));
    }
}