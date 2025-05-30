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
                try (Connection connection = coreConnectionFactory.create(ConnectionOptions.builder().database(null).build()); Statement statement = connection.createStatement()) {
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
     * We will detect the firebolt backend by checking which tags to be included/excluded.
     * If including: core -> then we want to run against core backend
     * If including: v1 -> then we run against cloud v1
     * If including: v2 -> then we run against cloud v2
     * @return
     */
    private FireboltBackendType detectFireboltBackend() {
        Set<String> includeTags = getIncludeTags();
        if (includeTags.isEmpty()) {
            throw new RuntimeException("Please specify which backend you want your tests to run. Use -DincludeTags system parameter to include the backend (v1 or v2 or core)");
        }

        // -DincludeTags=core  -> then run against core
        if (includeTags.contains(TestTag.CORE) && !includeTags.contains(TestTag.V1) && !includeTags.contains(TestTag.V2)) {
            return FireboltBackendType.FIREBOLT_CORE;
        }

        // -DincludeTags=v2  -> then run against cloud v2
        if (includeTags.contains(TestTag.V2) && !includeTags.contains(TestTag.CORE) && !includeTags.contains(TestTag.V1)) {
            return FireboltBackendType.CLOUD_2_0;
        }

        // -DincludeTags=v1  -> then run against cloud v1
        if (includeTags.contains(TestTag.V1) && !includeTags.contains(TestTag.CORE) && !includeTags.contains(TestTag.V2)) {
            return FireboltBackendType.CLOUD_1_0;
        }

        // -DincludeTags=v1,core or -DincludeTags=v1,v2 -> then cannot detect against what we are going to run against
        throw new RuntimeException("Cannot detect against which backend you want to run your tests. Use -DincludeTags system parameter to specify the backend (v1 or v2 or core): -DincludeTags=v2");
    }

    /**
     * The way that our integration workflow are set up is to use the -DincludeTags to specify the tests groups to run (of which we should include one of: v1, v2 or core) to know against which backend the tests are running
     * @return
     */
    private Set<String> getIncludeTags() {
        String includeTags = System.getProperty("includeTags", "core");
        String[] tags = includeTags.split(",");

        Set<String> allSupportedTags = TestTag.getAllSupportedTags();
        return Arrays.stream(tags).filter(tag -> allSupportedTags.contains(tag))
                .collect(Collectors.toSet());
    }
}
