package com.firebolt.jdbc.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Starting from JaCoCo 0.8.2, we can exclude classes and methods by annotating them with a custom annotation with the following properties:
 * <ul>
 * <li>The name of the annotation should include {@code Generated}.</li>
 * <li>The retention policy of annotation should be runtime or class.</li>
 * </ul>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE, ElementType.CONSTRUCTOR, ElementType.METHOD })
public @interface ExcludeFromJacocoGeneratedReport {
}