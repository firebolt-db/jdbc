package integration.tests;

import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import integration.IntegrationTest;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class TimeoutTest extends IntegrationTest {

	@Test
	@Timeout(value = 7, unit = TimeUnit.MINUTES)
	void shouldExecuteRequestWithoutTimeout() {
		long startTime = System.nanoTime();
		try (Connection con = this.createConnection(); Statement stmt = con.createStatement()) {
			this.setParam(con, "use_standard_sql", "0");
			this.setParam(con, "advanced_mode", "1");
			stmt.executeQuery("SELECT sleepEachRow(1) from numbers(360)");
		} catch (Exception e) {
			log.error("Error", e);
			fail();
		}
		log.info("Time elapsed: " + (System.nanoTime() - startTime) / 1_000_000_000 + " seconds");
	}
}
