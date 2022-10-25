//package com.firebolt.jdbc.client.authentication;
//
//import static org.junit.jupiter.api.Assertions.assertEquals;
//import static org.junit.jupiter.api.Assertions.assertThrows;
//import static org.mockito.ArgumentMatchers.any;
//import static org.mockito.Mockito.*;
//
//import java.io.ByteArrayInputStream;
//import java.io.IOException;
//import java.io.InputStream;
//
//import org.apache.hc.client5.http.classic.methods.HttpPost;
//import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
//import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
//import org.apache.hc.core5.http.HttpEntity;
//import org.apache.hc.core5.http.HttpStatus;
//import org.apache.hc.core5.http.ParseException;
//import org.junit.jupiter.api.AfterAll;
//import org.junit.jupiter.api.BeforeAll;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import org.junit.jupiter.api.extension.ExtendWith;
//import org.junitpioneer.jupiter.SetSystemProperty;
//import org.mockito.*;
//import org.mockito.junit.jupiter.MockitoExtension;
//
//import com.fasterxml.jackson.databind.DeserializationFeature;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.firebolt.jdbc.VersionUtil;
//import com.firebolt.jdbc.client.authentication.response.FireboltAuthenticationResponse;
//import com.firebolt.jdbc.connection.FireboltConnection;
//import com.firebolt.jdbc.exception.FireboltException;
//
//@SetSystemProperty(key = "java.version", value = "8.0.1")
//@SetSystemProperty(key = "os.version", value = "10.1")
//@SetSystemProperty(key = "os.name", value = "MacosX")
//@ExtendWith(MockitoExtension.class)
//class FireboltAuthenticationClientTest {
//	private static final String HOST = "https://host";
//	private static final String USER = "usr";
//	private static final String PASSWORD = "PA§§WORD";
//
//	private static MockedStatic<VersionUtil> mockedProjectVersionUtil;
//
//	@Spy
//	private final ObjectMapper objectMapper = new ObjectMapper()
//			.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
//
//	@Captor
//	ArgumentCaptor<HttpPost> httpPostArgumentCaptor;
//	@Mock
//	private CloseableHttpClient httpClient;
//	private FireboltAuthenticationClient fireboltAuthenticationClient;
//
//	@Mock
//	private FireboltConnection connection;
//
//	@BeforeAll
//	static void init() {
//		mockedProjectVersionUtil = mockStatic(VersionUtil.class);
//		mockedProjectVersionUtil.when(VersionUtil::getDriverVersion).thenReturn("1.0-TEST");
//	}
//
//	@AfterAll
//	public static void close() {
//		mockedProjectVersionUtil.close();
//	}
//
//	@BeforeEach
//	void setUp() {
//		fireboltAuthenticationClient = new FireboltAuthenticationClient(httpClient, objectMapper, connection,
//				"ConnA:1.0.9", "ConnB:2.0.9");
//	}
//
//	@Test
//	void shouldPostConnectionTokens() throws IOException, ParseException, FireboltException {
//		CloseableHttpResponse response = mock(CloseableHttpResponse.class);
//		HttpEntity entity = mock(HttpEntity.class);
//		when(response.getEntity()).thenReturn(entity);
//		when(response.getCode()).thenReturn(HttpStatus.SC_OK);
//		when(httpClient.execute(any())).thenReturn(response);
//		InputStream tokensResponse = new ByteArrayInputStream(new ObjectMapper().writeValueAsBytes(
//				FireboltAuthenticationResponse.builder().accessToken("a").refreshToken("r").expiresIn(1).build()));
//		when(entity.getContent()).thenReturn(tokensResponse);
//
//		fireboltAuthenticationClient.postConnectionTokens(HOST, USER, PASSWORD);
//
//		verify(httpClient).execute(httpPostArgumentCaptor.capture());
//		HttpPost actualPost = httpPostArgumentCaptor.getValue();
//		assertEquals("User-Agent", actualPost.getHeaders()[0].getName());
//		assertEquals("ConnB/2.0.9 JDBC/1.0-TEST (Java 8.0.1; Darwin 10.1; ) ConnA/1.0.9",
//				actualPost.getHeaders()[0].getValue());
//		verify(objectMapper).readValue("{\"access_token\":\"a\",\"refresh_token\":\"r\",\"expires_in\":1}",
//				FireboltAuthenticationResponse.class);
//	}
//
//	@Test
//	void shouldThrowExceptionWhenStatusCodeIsNotFound() throws Exception {
//		CloseableHttpResponse response = mock(CloseableHttpResponse.class);
//		HttpEntity entity = mock(HttpEntity.class);
//		when(response.getEntity()).thenReturn(entity);
//		when(response.getCode()).thenReturn(HttpStatus.SC_NOT_FOUND);
//		when(httpClient.execute(any())).thenReturn(response);
//
//		assertThrows(FireboltException.class,
//				() -> fireboltAuthenticationClient.postConnectionTokens(HOST, USER, PASSWORD));
//	}
//
//	@Test
//	void shouldThrowExceptionWhenStatusCodeIsNotOk() throws Exception {
//		CloseableHttpResponse response = mock(CloseableHttpResponse.class);
//		when(response.getCode()).thenReturn(HttpStatus.SC_BAD_GATEWAY);
//		HttpEntity entity = mock(HttpEntity.class);
//		when(response.getEntity()).thenReturn(entity);
//		when(httpClient.execute(any())).thenReturn(response);
//
//		assertThrows(FireboltException.class,
//				() -> fireboltAuthenticationClient.postConnectionTokens(HOST, USER, PASSWORD));
//	}
//
//	@Test
//	void shouldThrowExceptionWhenStatusCodeIsForbidden() throws Exception {
//		CloseableHttpResponse response = mock(CloseableHttpResponse.class);
//		when(response.getCode()).thenReturn(HttpStatus.SC_FORBIDDEN);
//		HttpEntity entity = mock(HttpEntity.class);
//		when(response.getEntity()).thenReturn(entity);
//		when(httpClient.execute(any())).thenReturn(response);
//
//		assertThrows(FireboltException.class,
//				() -> fireboltAuthenticationClient.postConnectionTokens(HOST, USER, PASSWORD));
//	}
//}
