package integration.tests;

import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import integration.IntegrationTest;
import lombok.CustomLog;

@CustomLog
class TimeoutTest extends IntegrationTest {

	@Test
	@Timeout(value = 7, unit = TimeUnit.MINUTES)
	void shouldExecuteRequestWithoutTimeout() throws SQLException {
		long startTime = System.nanoTime();
		try (Connection con = this.createConnection(); Statement stmt = con.createStatement()) {
			String numbers = IntStream.range(1, 10_000).boxed().map(String::valueOf).collect(Collectors.joining(","));
			String query = String.format("WITH arr AS (SELECT [%s] as a)%nSELECT md5(md5(md5(md5(md5(md5(md5(to_string(a)))))))) FROM arr UNNEST(a)", numbers);
			stmt.executeQuery(query);
		} catch (Exception e) {
			log.error("Error", e);
			fail();
		}
		log.info("Time elapsed: " + (System.nanoTime() - startTime) / 1_000_000_000 + " seconds");
	}
}
