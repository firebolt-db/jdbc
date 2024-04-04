package integration;

import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.FireboltConnectionServiceSecret;
import com.firebolt.jdbc.connection.FireboltConnectionUserPassword;
import integration.EnvironmentCondition.Attribute;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.opentest4j.AssertionFailedError;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Supplier;

public class EnvironmentVersionCondition implements ExecutionCondition {
    private static int infraVersion = -1;
    private static String databaseVersion;
    private static String protocolVersion;
    private static String fireboltVersion;

    private static final Map<Class<? extends Connection>, String> connectionVersions = Map.of(
            FireboltConnectionUserPassword.class, "v1",
            FireboltConnectionServiceSecret.class, "v2"
    );

    private static Map<Attribute, Supplier<String>> attributeValueGetter = Map.of(
            Attribute.infraVersion, () -> Integer.toString(infraVersion),
            Attribute.databaseVersion, () -> databaseVersion,
            Attribute.protocolVersion, () -> protocolVersion,
            Attribute.fireboltVersion, () -> fireboltVersion
    );

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext extensionContext) {
        Object testCase = extensionContext.getRequiredTestInstance();
        if (!(testCase instanceof IntegrationTest)) {
            return ConditionEvaluationResult.enabled("Test enabled");
        }
        EnvironmentCondition[] environmentConditions = extensionContext.getElement()
                .map(a -> a.getAnnotationsByType(EnvironmentCondition.class))
                .orElse(new EnvironmentCondition[0]);
        if (environmentConditions.length == 0) {
            return ConditionEvaluationResult.enabled("Test enabled");
        }
        if (infraVersion < 0) {
            retrieveVersionAttributes((IntegrationTest)testCase);
        }

        boolean enabled = Arrays.stream(environmentConditions)
                .map(condition -> condition.comparison().test(attributeValueGetter.get(condition.attribute()).get(), condition.value()))
                .filter(r -> r)
                .findFirst()
                .orElse(false);
        return enabled ? ConditionEvaluationResult.enabled("Test enabled") : ConditionEvaluationResult.disabled("Test disabled");
    }

    private void retrieveVersionAttributes(IntegrationTest test) {
        try (FireboltConnection conn = (FireboltConnection)test.createConnection()) {
            infraVersion = conn.getInfraVersion();
            databaseVersion = conn.getMetaData().getDatabaseProductVersion();
            fireboltVersion = connectionVersions.get(conn.getClass());
            protocolVersion = conn.getProtocolVersion();
        } catch (SQLException e) {
            throw new AssertionFailedError("Cannot establish connection", e);
        }
    }
}
