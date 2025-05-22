package integration;

import com.firebolt.jdbc.testutils.TestTag;
import org.junit.jupiter.api.Tag;

/**
 * Extend this class for tests that will run against all backends: v1, v2 and core
 */
@Tag(TestTag.V1)
@Tag(TestTag.V2)
@Tag(TestTag.CORE)
public class CommonIntegrationTest extends IntegrationTest {
}
