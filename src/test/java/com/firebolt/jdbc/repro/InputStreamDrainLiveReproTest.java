package com.firebolt.jdbc.repro;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Optional Stage 3 live confirmation (env-gated). Skips when FIREBOLT_* vars are unset.
 * Only touches table {@code jdbc_repro_drop_me}. Caps runtime at 45 minutes.
 *
 * <p>Set {@code FIREBOLT_REPRO_IDLE_MS} (e.g. 360000–600000) and {@code FIREBOLT_REPRO_BATCHES}
 * to attempt idle-out RST between inserts; default is a short smoke (3 inserts, no idle).
 */
class InputStreamDrainLiveReproTest {

    private static boolean credentialsPresent() {
        return notBlank(env("FIREBOLT_CLIENT_ID"))
                && notBlank(env("FIREBOLT_CLIENT_SECRET"))
                && notBlank(env("FIREBOLT_ACCOUNT"))
                && notBlank(env("FIREBOLT_DATABASE"))
                && notBlank(env("FIREBOLT_ENGINE"));
    }

    private static boolean notBlank(String s) {
        return s != null && !s.isBlank();
    }

    private static String env(String key) {
        return System.getenv(key);
    }

    private Connection openConnection() throws SQLException {
        String account = env("FIREBOLT_ACCOUNT");
        String database = env("FIREBOLT_DATABASE");
        String engine = env("FIREBOLT_ENGINE");
        String environment = System.getenv().getOrDefault("FIREBOLT_ENVIRONMENT", "app");
        String url = "jdbc:firebolt:" + database
                + "?account=" + account
                + "&engine=" + engine
                + "&environment=" + environment;
        Properties props = new Properties();
        props.setProperty("client_id", env("FIREBOLT_CLIENT_ID"));
        props.setProperty("client_secret", env("FIREBOLT_CLIENT_SECRET"));
        // Keep default connection_keep_alive_timeout_millis; reuse one Connection for batches.
        return DriverManager.getConnection(url, props);
    }

    @Test
    @Timeout(value = 45, unit = TimeUnit.MINUTES)
    void createInsertAndCleanupReproTable() throws Exception {
        Assumptions.assumeTrue(credentialsPresent(),
                "Skipping Stage 3: FIREBOLT_* env vars not set");

        System.out.println("Stage 3: live validation account=" + env("FIREBOLT_ACCOUNT")
                + " db=" + env("FIREBOLT_DATABASE")
                + " engine=" + env("FIREBOLT_ENGINE"));

        long idleBetweenBatchesMs = Long.parseLong(System.getenv().getOrDefault("FIREBOLT_REPRO_IDLE_MS", "0"));
        int batches = Integer.parseInt(System.getenv().getOrDefault("FIREBOLT_REPRO_BATCHES", "3"));

        try (Connection connection = openConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate("DROP TABLE IF EXISTS jdbc_repro_drop_me");
            statement.executeUpdate("CREATE TABLE jdbc_repro_drop_me (id int)");
            for (int b = 0; b < batches; b++) {
                try {
                    statement.executeUpdate("INSERT INTO jdbc_repro_drop_me VALUES (1)");
                    System.out.println("Stage 3: batch " + b + " INSERT ok");
                } catch (SQLException e) {
                    System.out.println("Stage 3: batch " + b + " INSERT failed: " + e);
                    e.printStackTrace(System.out);
                    throw e;
                }
                if (idleBetweenBatchesMs > 0 && b + 1 < batches) {
                    System.out.println("Stage 3: sleeping " + idleBetweenBatchesMs + "ms before next batch");
                    Thread.sleep(idleBetweenBatchesMs);
                }
            }
        } finally {
            if (credentialsPresent()) {
                try (Connection connection = openConnection();
                     Statement statement = connection.createStatement()) {
                    statement.executeUpdate("DROP TABLE IF EXISTS jdbc_repro_drop_me");
                    System.out.println("Stage 3: cleaned up jdbc_repro_drop_me");
                } catch (SQLException cleanupError) {
                    System.out.println("Stage 3: cleanup failed: " + cleanupError.getMessage());
                }
            }
        }
    }
}
