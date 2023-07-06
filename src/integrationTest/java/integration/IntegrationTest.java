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

	private static final String JDBC_URL_PREFIX = "jdbc:firebolt:";

	protected Connection createLocalConnection(String queryParams) throws SQLException {
		return DriverManager.getConnection(
				JDBC_URL_PREFIX + integration.ConnectionInfo.getInstance().getDatabase()
						+ queryParams + "&host=localhost" + getAccountParam(),
				integration.ConnectionInfo.getInstance().getPrincipal(),
				integration.ConnectionInfo.getInstance().getSecret());
	}

	protected Connection createConnection() throws SQLException {
		return DriverManager.getConnection(
				JDBC_URL_PREFIX
						+ integration.ConnectionInfo.getInstance().getDatabase() + "?" + getEnvParam() + getAccountParam() ,
				integration.ConnectionInfo.getInstance().getPrincipal(),
				integration.ConnectionInfo.getInstance().getSecret());
	}

	protected Connection createConnection(String engine) throws SQLException {
		return DriverManager.getConnection(
				JDBC_URL_PREFIX +
						 integration.ConnectionInfo.getInstance().getDatabase()
						+ Optional.ofNullable(engine).map(e -> "?" + getEnvParam() +"&engine=" + e + getAccountParam() ).orElse("?" + getEnvParam() + getAccountParam()),
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

	private String getAccountParam() {
		return "&account=" + integration.ConnectionInfo.getInstance().getAccount();
	}

	private String getEnvParam() {
		return "&env=" + integration.ConnectionInfo.getInstance().getEnv();
	}

}
