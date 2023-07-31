package integration;

import com.firebolt.jdbc.client.HttpClientConfig;
import lombok.CustomLog;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static com.firebolt.jdbc.connection.FireboltConnectionUserPassword.SYSTEM_ENGINE_NAME;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@CustomLog
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Tag("common")
public abstract class IntegrationTest {

	private static final String JDBC_URL_PREFIX = "jdbc:firebolt:";

	protected Connection createLocalConnection(String queryParams) throws SQLException {
		return DriverManager.getConnection(
				JDBC_URL_PREFIX + integration.ConnectionInfo.getInstance().getDatabase()
						+ queryParams + "&host=localhost" + getAccountParam(),
				integration.ConnectionInfo.getInstance().getPrincipal(),
				integration.ConnectionInfo.getInstance().getSecret());
	}

	protected Connection createConnection() throws SQLException {
		return DriverManager.getConnection(integration.ConnectionInfo.getInstance().toJdbcUrl(),
				integration.ConnectionInfo.getInstance().getPrincipal(),
				integration.ConnectionInfo.getInstance().getSecret());
	}

	protected Connection createConnection(String engine) throws SQLException {
		ConnectionInfo current = integration.ConnectionInfo.getInstance();
		ConnectionInfo updated = new ConnectionInfo(current.getPrincipal(), current.getSecret(),
				current.getEnv(), current.getDatabase(), current.getAccount(), engine, current.getApi());
		return DriverManager.getConnection(updated.toJdbcUrl(),
				integration.ConnectionInfo.getInstance().getPrincipal(),
				integration.ConnectionInfo.getInstance().getSecret());
	}

	protected void setParam(Connection connection, String name, String value) throws SQLException {
		try (Statement statement = connection.createStatement()) {
			statement.execute(String.format("set %s = %s", name, value));
		}
	}

	@SneakyThrows
	protected void executeStatementFromFile(String path) {
		executeStatementFromFile(path, integration.ConnectionInfo.getInstance().getEngine());
	}

	@SneakyThrows
	protected void executeStatementFromFile(String path, String engine) {
		try (Connection connection = createConnection(engine); Statement statement = connection.createStatement(); InputStream is = IntegrationTest.class.getResourceAsStream(path)) {
			assertNotNull(is);
			String sql = new String(is.readAllBytes(), StandardCharsets.UTF_8);
			statement.execute(sql);
		}
	}

	protected void removeExistingClient() throws NoSuchFieldException, IllegalAccessException {
		Field field = HttpClientConfig.class.getDeclaredField("instance");
		field.setAccessible(true);
		field.set(null, null);
	}

	private String getAccountParam() {
		return "&account=" + integration.ConnectionInfo.getInstance().getAccount();
	}

	protected String getSystemEngineName() {
		return System.getProperty("api") == null ? null : SYSTEM_ENGINE_NAME;
	}
}
