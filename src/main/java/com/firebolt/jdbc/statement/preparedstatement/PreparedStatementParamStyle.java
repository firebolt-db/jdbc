package com.firebolt.jdbc.statement.preparedstatement;


import lombok.Getter;

@Getter
public enum PreparedStatementParamStyle {
    NATIVE('?'),
    FB_NUMERIC('$'),;

    private final char queryParam;

    PreparedStatementParamStyle(char queryParam) {
        this.queryParam = queryParam;
    }

    public static PreparedStatementParamStyle fromString(String propertyName) {
        for (PreparedStatementParamStyle style : PreparedStatementParamStyle.values()) {
            if (style.name().equalsIgnoreCase(propertyName)) {
                return style;
            }
        }
        throw new IllegalArgumentException("Unknown PreparedStatementParamStyle: " + propertyName);
    }
}
