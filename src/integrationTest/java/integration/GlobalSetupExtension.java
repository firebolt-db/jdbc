
package integration;

import com.firebolt.jdbc.FireboltBackendType;
import com.firebolt.jdbc.testutils.TestTag;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.CustomLog;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * JUnit does not support a test annotation similar to @BeforeSuite from TestNG that will execute a piece of code once, before any test is run
 * JUnit offers the Extension mechanism that can be used as a workaround to offer the same functionality
 */
@CustomLog
public class GlobalSetupExtension implements BeforeAllCallback {

    private static boolean initialized = false;

    static {
        log.info("GlobalSetupExtension class loaded");
    }

    @Override
    public void beforeAll(ExtensionContext context) throws SQLException {
        log.info("GlobalSetupExtension.beforeAll called for context: {}", context.getDisplayName());
        if (!initialized) {
            log.info("Setting up the connection factory");

            FireboltBackendType fireboltBackend = detectFireboltBackend();

            if (FireboltBackendType.FIREBOLT_CORE == fireboltBackend) {
                log.info("Using the Firebolt core as the backend");
                ConnectionFactory coreConnectionFactory = new CoreConnectionFactory();
                IntegrationTest.setConnectionFactory(coreConnectionFactory);

                // need to create the default database if it does not exist
                log.info("Setting up the default database");
                try (Connection connection = coreConnectionFactory.create(); Statement statement = connection.createStatement()) {
                    statement.execute("DROP DATABASE IF EXISTS " + coreConnectionFactory.getDefaultDatabase());
                    statement.execute("CREATE DATABASE IF NOT EXISTS " + coreConnectionFactory.getDefaultDatabase());
                }

                log.info("Database {} has been created", coreConnectionFactory.getDefaultDatabase());
            } else if (FireboltBackendType.CLOUD_1_0 == fireboltBackend || FireboltBackendType.CLOUD_2_0 == fireboltBackend) {
                log.info("Using the Firebolt cloud as the backend");
                IntegrationTest.setConnectionFactory(new CloudConnectionFactory());
            }

            initialized = true;
        }
    }

    /**
     * We will detect the firebolt backend by checking which tags to be excluded.
     * If excluding: v1,v2 -> then we want to run against core backend
     * If excluding: v1,core -> then we run against cloud v1
     * If excluding: v2,core -> then we run against cloud v2
     * @return
     */
    private FireboltBackendType detectFireboltBackend() {
        Set<String> excludeTags = getExcludeTags();
        if (excludeTags.isEmpty()) {
            throw new RuntimeException("Please specify which backend you want your tests to run. Use -DexcludeTags system parameter to exclude the backend (v1 or v2 or core)");
        }

        // -DexcludeTags=v1,v2  -> then run against core
        if (!excludeTags.contains(TestTag.CORE) && excludeTags.contains(TestTag.V1) && excludeTags.contains(TestTag.V2)) {
            return FireboltBackendType.FIREBOLT_CORE;
        }

        // -DexcludeTags=core,v1  -> then run against cloud v2
        if (excludeTags.contains(TestTag.CORE) && excludeTags.contains(TestTag.V1) && !excludeTags.contains(TestTag.V2)) {
            return FireboltBackendType.CLOUD_2_0;
        }

        // -DexcludeTags=core,v2  -> then run against cloud v1
        if (excludeTags.contains(TestTag.CORE) && excludeTags.contains(TestTag.V2) && !excludeTags.contains(TestTag.V1)) {
            return FireboltBackendType.CLOUD_1_0;
        }

        // -DexcludeTags=v2 or -DexcludeTags=v1 -> then cannot detect against what we are going to run against
        throw new RuntimeException("Cannot detect against which backend you want to run your tests. Use -DexcludeTags system parameter to exclude the backend (v1 or v2 or core)");
    }

    /**
     * The way that our integration workflow are setup is to use the -DexcludeTag to exclude the version of backend that we don't want in the current run.
     * @return
     */
    private Set<String> getExcludeTags() {
        String excludeTags = System.getProperty("excludeTags", "");
        String[] tags = excludeTags.split(",");

        Set<String> allSupportedTags = TestTag.getAllSupportedTags();
        return Arrays.stream(tags).filter(tag -> allSupportedTags.contains(tag))
                .collect(Collectors.toSet());
    }
}
