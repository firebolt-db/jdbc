package integration;

import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ ElementType.TYPE, ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(InfraVersionCondition.class)
public @interface InfraVersion {
    int value();
    Comparison comparison();
    enum Comparison {
        LT {
            boolean test(int value, int threshold) {
                return value < threshold;
            }
        },
        LE {
            boolean test(int value, int threshold) {
                return value <= threshold;
            }
        },
        EQ {
            boolean test(int value, int threshold) {
                return value == threshold;
            }
        },
        NE {
            boolean test(int value, int threshold) {
                return value != threshold;
            }
        },
        GE {
            boolean test(int value, int threshold) {
                return value >= threshold;
            }
        },
        GT {
            boolean test(int value, int threshold) {
                return value == threshold;
            }
        },
        ;
        abstract boolean test(int value, int threshold);
    }
}
