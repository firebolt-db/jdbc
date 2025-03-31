package com.firebolt.jdbc.type;

import com.firebolt.jdbc.exception.FireboltException;
import lombok.CustomLog;

import static java.lang.String.format;

@CustomLog
class BooleanTypeUtil {

    private BooleanTypeUtil() {
    }

    static String castToFireboltBoolean(Object value) throws FireboltException {
        return castToBoolean(value) ? "true" : "false";
    }

    /**
     * Cast an Object value to the corresponding boolean value.
     *
     * @param in Object to cast into boolean
     * @return boolean value corresponding to the cast of the object
     * @throws FireboltException cannot
     */
    @SuppressWarnings("java:S6201")
    static boolean castToBoolean(final Object in) throws FireboltException {
        log.debug("Cast to boolean: \"{0}\"", String.valueOf(in));
        if (in instanceof Boolean) {
            return (Boolean) in;
        }
        if (in instanceof String) {
            return fromString((String) in);
        }
        if (in instanceof Character) {
            return fromCharacter((Character) in);
        }
        if (in instanceof Number) {
            return fromNumber((Number) in);
        }
        throw new FireboltException(format("Cannot cast %s to boolean", in));
    }

    static boolean fromString(final String strval) throws FireboltException {
        // Leading or trailing whitespace is ignored, and case does not matter.
        final String val = strval.trim();
        if ("1".equals(val) || "true".equalsIgnoreCase(val)
                || "t".equalsIgnoreCase(val) || "yes".equalsIgnoreCase(val)
                || "y".equalsIgnoreCase(val) || "on".equalsIgnoreCase(val)) {
            return true;
        }
        if ("0".equals(val) || "false".equalsIgnoreCase(val)
                || "f".equalsIgnoreCase(val) || "no".equalsIgnoreCase(val)
                || "n".equalsIgnoreCase(val) || "off".equalsIgnoreCase(val)) {
            return false;
        }
        throw cannotCastException(strval);
    }

    private static boolean fromCharacter(final Character charval) throws FireboltException {
        if ('1' == charval || 't' == charval || 'T' == charval
                || 'y' == charval || 'Y' == charval) {
            return true;
        }
        if ('0' == charval || 'f' == charval || 'F' == charval
                || 'n' == charval || 'N' == charval) {
            return false;
        }
        throw cannotCastException(charval);
    }

    private static boolean fromNumber(final Number numval) throws FireboltException {
        // Handles BigDecimal, Byte, Short, Integer, Long Float, Double
        // based on the widening primitive conversions.
        final double value = numval.doubleValue();
        if (value == 1.0d) {
            return true;
        }
        if (value == 0.0d) {
            return false;
        }
        throw cannotCastException(numval);
    }

    private static FireboltException cannotCastException(final Object value) {
        return new FireboltException(format("Cannot cast to boolean: \"%s\"", value));
    }

}
