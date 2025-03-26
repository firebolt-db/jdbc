package integration.tests.client;

import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.util.VersionUtil;
import integration.MockWebServerAwareIntegrationTest;
import java.sql.SQLException;
import java.sql.Statement;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UsageTrackingTest extends MockWebServerAwareIntegrationTest {
	@Test
	public void shouldSendRequestWithUserAgentHeaderContainingDriverAndClientInfo()
			throws SQLException, InterruptedException {
		mockBackEnd.enqueue(new MockResponse().setResponseCode(200));
		mockBackEnd.enqueue(new MockResponse().setResponseCode(200));

		try (FireboltConnection fireboltConnection = (FireboltConnection) createLocalConnection(String.format(
				"?ssl=0&port=%d&max_retries=%d&user_drivers=AwesomeDriver:1.0.1&user_clients=GreatClient:0.1.4&access_token=token1",
				mockBackEnd.getPort(), 3)); Statement statement = fireboltConnection.createStatement()) {
			statement.execute("SELECT 1;");
			RecordedRequest request = mockBackEnd.takeRequest();
			String userAgentHeader = request.getHeaders().get("User-Agent");
			assertNotNull(userAgentHeader);
			assertTrue(userAgentHeader.startsWith("GreatClient/0.1.4" + " JDBC/" + VersionUtil.getDriverVersion()));
			assertTrue(userAgentHeader.contains("AwesomeDriver/1.0.1"));

			// connection is not cached so there should be only connId set
			assertTrue(userAgentHeader.contains("connId"));
			assertFalse(userAgentHeader.contains("cachedConnId"));
		}

		// create a second connection
		try (FireboltConnection fireboltConnection = (FireboltConnection) createLocalConnection(String.format(
				"?ssl=0&port=%d&max_retries=%d&user_drivers=AwesomeDriver:1.0.1&user_clients=GreatClient:0.1.4&access_token=token1",
				mockBackEnd.getPort(), 3)); Statement statement = fireboltConnection.createStatement()) {
			statement.execute("SELECT 1;");
			RecordedRequest request = mockBackEnd.takeRequest();
			String userAgentHeader = request.getHeaders().get("User-Agent");
			assertNotNull(userAgentHeader);
			assertTrue(userAgentHeader.startsWith("GreatClient/0.1.4" + " JDBC/" + VersionUtil.getDriverVersion()));
			assertTrue(userAgentHeader.contains("AwesomeDriver/1.0.1"));

			// connection should be cached
			assertTrue(userAgentHeader.contains("connId"));
			assertTrue(userAgentHeader.contains("cachedConnId"));
		}

	}

}
