package integration.tests.client;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.util.VersionUtil;

import integration.IntegrationTest;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;

public class UsageTrackingTest extends IntegrationTest {

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
	public void shouldSendRequestWithUserAgentHeaderContainingDriverAndClientInfo()
			throws SQLException, InterruptedException {
		mockBackEnd.enqueue(new MockResponse().setResponseCode(200));
		try (FireboltConnection fireboltConnection = (FireboltConnection) createLocalConnection(String.format(
				"?ssl=0&port=%d&max_retries=%d&user_drivers=AwesomeDriver:1.0.1&user_clients=GreatClient:0.1.4",
				mockBackEnd.getPort(), 3)); Statement statement = fireboltConnection.createStatement()) {
			statement.execute("SELECT 1;");
			RecordedRequest request = mockBackEnd.takeRequest();
			String userAgentHeader = request.getHeaders().get("User-Agent");
			assertTrue(StringUtils.startsWith(userAgentHeader,
					"GreatClient/0.1.4" + " JDBC/" + VersionUtil.getDriverVersion()));
			assertTrue(StringUtils.endsWith(userAgentHeader, "AwesomeDriver/1.0.1"));
		}
	}

}
