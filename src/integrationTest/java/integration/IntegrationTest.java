package integration;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;

import org.junit.jupiter.api.TestInstance;

import com.firebolt.jdbc.client.HttpClientConfig;
import com.google.common.io.Resources;

import lombok.CustomLog;
import lombok.SneakyThrows;

@CustomLog
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class IntegrationTest {

	protected Connection createLocalConnection(String queryParams) throws SQLException {
		return DriverManager.getConnection(
				"jdbc:firebolt://localhost" + "/" + integration.ConnectionInfo.getInstance().getDatabase()
						+ queryParams,
				integration.ConnectionInfo.getInstance().getUser(),
				integration.ConnectionInfo.getInstance().getPassword());
	}

	protected Connection createConnection() throws SQLException {
		return DriverManager.getConnection(
				"jdbc:firebolt://" + integration.ConnectionInfo.getInstance().getApi() + "/"
						+ integration.ConnectionInfo.getInstance().getDatabase() + "?compress=0&log_result_set=1",
				integration.ConnectionInfo.getInstance().getUser(),
				integration.ConnectionInfo.getInstance().getPassword());
	}

	protected Connection createConnection(String engine) throws SQLException {
		return DriverManager.getConnection(
				"jdbc:firebolt://" + integration.ConnectionInfo.getInstance().getApi() + "/"
						+ integration.ConnectionInfo.getInstance().getDatabase()
						+ Optional.ofNullable(engine).map(e -> "?engine=" + e).orElse(""),
				integration.ConnectionInfo.getInstance().getUser(),
				integration.ConnectionInfo.getInstance().getPassword());
	}

	protected void setParam(Connection connection, String name, String value) throws SQLException {
		try (Statement statement = connection.createStatement()) {
			statement.execute(String.format("set %s = %s", name, value));
		}
	}

	@SneakyThrows
	protected void executeStatementFromFile(String path) {
		executeStatementFromFile(path, null);
	}

	@SneakyThrows
	protected void executeStatementFromFile(String path, String engine) {
		try (Connection connection = createConnection(engine); Statement statement = connection.createStatement()) {
			String sql = Resources.toString(IntegrationTest.class.getResource(path), StandardCharsets.UTF_8);
			statement.execute(sql);
		}
	}

	protected void removeExistingClient() throws NoSuchFieldException, IllegalAccessException {
		Field field = HttpClientConfig.class.getDeclaredField("instance");
		field.setAccessible(true);
		field.set(null, null);
	}

}
