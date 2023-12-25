package integration;

import com.firebolt.jdbc.connection.FireboltConnection;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.opentest4j.AssertionFailedError;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;

public class InfraVersionCondition implements ExecutionCondition {
    private static int infraVersion = -1;

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext extensionContext) {
        Object testCase = extensionContext.getRequiredTestInstance();
        if (!(testCase instanceof IntegrationTest)) {
            return ConditionEvaluationResult.enabled("Test enabled");
        }
        Optional<InfraVersion> infraVersionAnnotationOpt = extensionContext.getElement().map(a -> a.getAnnotation(InfraVersion.class));
        if (infraVersionAnnotationOpt.isEmpty()) {
            return ConditionEvaluationResult.enabled("Test enabled");
        }
        InfraVersion infraVersionAnnotation = infraVersionAnnotationOpt.get();
        if (infraVersion < 0) {
            infraVersion = retrieveInfraVersion((IntegrationTest)testCase);
        }
        boolean enabled = infraVersionAnnotation.comparison().test(infraVersion, infraVersionAnnotation.value());
        return enabled ? ConditionEvaluationResult.enabled("Test enabled") : ConditionEvaluationResult.disabled("Test disabled");
    }

    private int retrieveInfraVersion(IntegrationTest test) {
        try (Connection conn = test.createConnection()) {
            return ((FireboltConnection) conn).getInfraVersion();
        } catch (SQLException e) {
            throw new AssertionFailedError("Cannot establish connection", e);
        }
    }
}
