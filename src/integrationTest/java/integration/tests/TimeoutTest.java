package integration.tests;

import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.testutils.TestTag;
import integration.EnvironmentCondition;
import integration.IntegrationTest;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static integration.EnvironmentCondition.Attribute.databaseVersion;
import static integration.EnvironmentCondition.Comparison.GE;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertTrue;

@CustomLog
class TimeoutTest extends IntegrationTest {
	private static final int MIN_TIME_SECONDS = 350;
	private static final Map<Integer, Long> SERIES_SIZE = new HashMap<>();

	private long startTime;

	/**
	 * From GitHub we might pass in different values rather than the default ones that we use in tests. Sometimes we see the tests finish
	 * faster than we expect (350 seconds)
	 */
	@BeforeAll
	void setupClass() {
		long v1SeriesSize = Long.valueOf(System.getProperty("v1GenerateSeriesMaxSize", "80000000000"));
		SERIES_SIZE.put(1, v1SeriesSize);
		log.info("For v1 max generate series size we will use {}", v1SeriesSize);

		long v2SeriesSize = Long.valueOf(System.getProperty("v2GenerateSeriesMaxSize", "500000000000"));
		SERIES_SIZE.put(2, v2SeriesSize);
		log.info("For v2 max generate series size we will use {}", v2SeriesSize);
	}

	@BeforeEach
	void before() {
		startTime = System.nanoTime();
	}

	@AfterEach
	void after() {
		long endTime = System.nanoTime();
		long elapsedTimeSeconds = (endTime - startTime) / 1_000_000_000;
		log.info("Time elapsed: {} seconds", elapsedTimeSeconds);
		assertTrue(elapsedTimeSeconds > MIN_TIME_SECONDS, format("Test is too short. It took %d but should take at least %d seconds", elapsedTimeSeconds, MIN_TIME_SECONDS));
	}

	@Test
	@Timeout(value = 15, unit = TimeUnit.MINUTES)
	@Tag(TestTag.V1)
	@Tag(TestTag.SLOW)
	void shouldExecuteRequestWithoutTimeoutV1() throws SQLException {
		shouldExecuteRequestWithoutTimeout();
	}

	@Test
	@Timeout(value = 15, unit = TimeUnit.MINUTES)
	@EnvironmentCondition(value = "3.33", attribute = databaseVersion, comparison = GE) // generate_series is supported starting from version 3.33 on v2
	@Tag(TestTag.V2)
//	@Tag(TestTag.CORE) - this fails, against core. Will be fixed in next review
	@Tag(TestTag.SLOW)
	void shouldExecuteRequestWithoutTimeoutV2() throws SQLException {
		shouldExecuteRequestWithoutTimeout();
	}

	private void shouldExecuteRequestWithoutTimeout() throws SQLException {
		try (Connection con = createConnection(); Statement stmt = con.createStatement()) {
			int infraVersion = ((FireboltConnection)con).getInfraVersion();
			stmt.executeQuery(format("SELECT (max(x) - min(x))/count(x) + avg(x) FROM generate_series(1,%d) r(x)", SERIES_SIZE.get(infraVersion)));
		}
	}
}
