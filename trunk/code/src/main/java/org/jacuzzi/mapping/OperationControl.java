package org.jacuzzi.mapping;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author Maxim Shipko (sladethe@gmail.com)
 *         Date: 05.12.14
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD})
public @interface OperationControl {
    boolean ignoreSelect() default false;
    boolean ignoreInsert() default false;
    boolean ignoreUpdate() default false;
}
