package integration;

import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ ElementType.TYPE, ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(EnvironmentVersionCondition.class)
@Repeatable(EnvironmentConditions.class)
public @interface EnvironmentCondition {
    String value();
    Attribute attribute() default Attribute.infraVersion;
    Comparison comparison();
    enum Comparison {
        LT {
            boolean test(String value, String threshold) {
                return compare(value, threshold) < 0;
            }
        },
        LE {
            boolean test(String value, String threshold) {
                return compare(value, threshold) <= 0;
            }
        },
        EQ {
            boolean test(String value, String threshold) {
                return compare(value, threshold) == 0;
            }
        },
        NE {
            boolean test(String value, String threshold) {
                return compare(value, threshold) != 0;
            }
        },
        GE {
            boolean test(String value, String threshold) {
                return compare(value, threshold) >= 0;
            }
        },
        GT {
            boolean test(String value, String threshold) {
                return compare(value, threshold) > 0;
            }
        },
        ;

        protected int compare(String value, String threshold) {
            return value.compareTo(threshold);
        }
        abstract boolean test(String value, String threshold);
    }
    enum Attribute {
        infraVersion,
        databaseVersion,
        protocolVersion,
        fireboltVersion,
    }
}
