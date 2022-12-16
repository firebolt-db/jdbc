package integration.tests.client;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.InetAddress;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.firebolt.jdbc.connection.FireboltConnection;

import integration.IntegrationTest;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.tls.HandshakeCertificates;
import okhttp3.tls.HeldCertificate;

public class TLSTest extends IntegrationTest {
	private MockWebServer mockBackEnd;

	@BeforeEach
	void setUp() throws IOException {
		mockBackEnd = new MockWebServer();
		mockBackEnd.start();
	}

	@AfterEach
	void tearDown() throws IOException {
		mockBackEnd.close();
	}

	@Test
	public void shouldUseTLS() throws SQLException, IOException, NoSuchFieldException, IllegalAccessException {
		mockBackEnd.enqueue(new MockResponse().setResponseCode(200));

		/*
		 * The code below generates TLS certificates for the mock server based on
		 * https://github.com/square/okhttp/blob/
		 * 052a73d3abe536c0f04b5c7b04d35123cc2500a8/okhttp-tls/README.md
		 */

		String localhost = InetAddress.getByName("localhost").getCanonicalHostName();

		// HeldCertificate represents a certificate and its private key
		HeldCertificate localhostCertificate = new HeldCertificate.Builder().addSubjectAlternativeName(localhost)
				.build();

		// HandshakeCertificates contains certificates for a TLS handshake
		HandshakeCertificates serverCertificates = new HandshakeCertificates.Builder()
				.heldCertificate(localhostCertificate).addPlatformTrustedCertificates().build();

		mockBackEnd.useHttps(serverCertificates.sslSocketFactory(), false);

		// Write the public certificate to a file that will be used by the driver for
		// the TLS handshake
		String path = this.getClass().getResource("/").getPath() + UUID.randomUUID() + ".pem";
		try (Writer out = new FileWriter(new File(path).getAbsoluteFile())) {
			out.write(localhostCertificate.certificatePem());
		}
		removeExistingClient();
		try (FireboltConnection fireboltConnection = (FireboltConnection) createLocalConnection(
				String.format("?ssl_certificate_path=%s&port=%s", path, mockBackEnd.getPort()));
				Statement statement = fireboltConnection.createStatement()) {
			statement.execute("SELECT 1;");
			assertEquals(1, mockBackEnd.getRequestCount());
		} finally {
			removeExistingClient();
		}
	}
}
