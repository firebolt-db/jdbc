package integration.tests.client;

import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.exception.FireboltException;
import integration.MockWebServerAwareIntegrationTest;
import java.sql.SQLException;
import java.sql.Statement;
import okhttp3.mockwebserver.MockResponse;
import org.junit.jupiter.api.Test;

import static com.firebolt.jdbc.exception.ExceptionType.INVALID_REQUEST;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RetryPolicyTest extends MockWebServerAwareIntegrationTest {
	@Test
	public void shouldThrowExceptionOn400WithoutRetry() throws SQLException {
		mockBackEnd.enqueue(new MockResponse().setResponseCode(400));
		try (FireboltConnection fireboltConnection = (FireboltConnection) createLocalConnection(
				format("?ssl=0&port=%d&max_retries=%d&access_token=my_token", mockBackEnd.getPort(), 3));
				Statement statement = fireboltConnection.createStatement()) {
			FireboltException ex = assertThrows(FireboltException.class, () -> statement.execute("SELECT 1;"));
			assertEquals(ex.getType(), INVALID_REQUEST);
			assertMockBackendRequestsCount(1);
		}
	}

	@Test
	public void shouldRetryOn502() throws SQLException {
		mockBackEnd.enqueue(new MockResponse().setResponseCode(502));
		mockBackEnd.enqueue(new MockResponse().setResponseCode(502));
		mockBackEnd.enqueue(new MockResponse().setResponseCode(200));
		try (FireboltConnection fireboltConnection = (FireboltConnection) createLocalConnection(
				format("?ssl=0&port=%d&max_retries=%d&access_token=my_token", mockBackEnd.getPort(), 3));
				Statement statement = fireboltConnection.createStatement()) {
			statement.execute("SELECT 1;");
			assertMockBackendRequestsCount(3);
		}
	}

}
