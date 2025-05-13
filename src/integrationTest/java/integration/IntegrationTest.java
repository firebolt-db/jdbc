package integration;

import com.firebolt.jdbc.client.HttpClientConfig;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Map;
import lombok.CustomLog;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import static com.firebolt.jdbc.connection.FireboltConnectionUserPassword.SYSTEM_ENGINE_NAME;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@CustomLog
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Tag("common")
@ExtendWith(GlobalSetupExtension.class)
public abstract class IntegrationTest {

	protected static final String JDBC_URL_PREFIX = "jdbc:firebolt:";

	// timestamp format on the backend
	private static ZoneId UTC_ZONE_ID = ZoneId.of("UTC"); // Change this to your desired timezone

	// Get the current time in the specified timezone
	private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";
	private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern(TIMESTAMP_FORMAT);

	private static ConnectionFactory connectionFactory;

	public static void setConnectionFactory(ConnectionFactory factory) {
		connectionFactory = factory;
	}

	protected Connection createConnection() throws SQLException {
		return connectionFactory.create(ConnectionOptions.builder()
				.database(ConnectionInfo.getInstance().getDatabase())
				.engine(ConnectionInfo.getInstance().getEngine())
				.build());
	}

	protected Connection createConnection(String engine) throws SQLException {
		return connectionFactory.create(ConnectionOptions.builder()
				.database(ConnectionInfo.getInstance().getDatabase())
				.engine(engine)
				.build());
	}

	protected Connection createConnection(String engine, String database) throws SQLException {
		return connectionFactory.create(ConnectionOptions.builder()
				.database(database)
				.engine(engine)
				.build());
	}

	protected ConnectionInfo getConnectionInfoWithEngineAndDatabase(String engine, String database) {
		ConnectionInfo current = ConnectionInfo.getInstance();
        return new ConnectionInfo(current.getPrincipal(), current.getSecret(),
                current.getEnv(), database, current.getAccount(), engine, current.getApi(), Collections.emptyMap());
	}

	protected Connection createConnection(String engine, Map<String, String> extra) throws SQLException {
		return connectionFactory.create(ConnectionOptions.builder()
				.database(ConnectionInfo.getInstance().getDatabase())
				.engine(engine)
				.connectionParams(extra)
				.build());
	}

	protected Connection createConnectionWithOptions(ConnectionOptions connectionOptions) throws SQLException {
		return connectionFactory.create(connectionOptions);
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

	protected String getSystemEngineName() {
		return System.getProperty("api") == null ? null : SYSTEM_ENGINE_NAME;
	}

	/**
	 * Assume the back end and the client have the same timestamp
	 */
	protected String getCurrentUTCTime() {
		return ZonedDateTime.now(UTC_ZONE_ID).format(DATE_TIME_FORMATTER);
	}

	protected void sleepForMillis(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			// do nothing
		}
	}

	protected String getDefaultDatabase() {
		return connectionFactory.getDefaultDatabase();
	}

}
