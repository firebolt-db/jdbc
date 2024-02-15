package com.firebolt.jdbc.util;


import lombok.experimental.UtilityClass;

import java.util.ArrayList;
import java.util.List;

@UtilityClass
public class StringUtil {
    private static final String[] EMPTY_STRING_ARRAY = {};
    private static final String[] SINGLE_STRING_ARRAY = { "" };

    public static String strip(String value, char c) {
        if (value == null || value.isEmpty()) {
            return value;
        }
        int begin = value.charAt(0) == c ? 1 : 0;
        int end = value.charAt(value.length() - 1) == c ? value.length() - 1 : value.length();
        return value.substring(begin, end);
    }

    /**
     * Based on {@code org.apache.commons.lang3.StringUtils#split(String, char)} with small modifications, simplification and optimization
     * @param str – the nullable String to parse
     * @param  separatorChar – the character used as the delimiter, null splits on whitespace
     * @return an array of parsed Strings, empty array if input is {@code null} and single element with empty string array if input is empty string
     */
    public static String[] splitAll(String str, char separatorChar) {
        if (str == null) {
            return EMPTY_STRING_ARRAY;
        }
        final int len = str.length();
        if (len == 0) {
            return SINGLE_STRING_ARRAY;
        }
        char[] chars = str.toCharArray();
        final List<String> list = new ArrayList<>();
        int i = 0;
        int start = 0;
        while (i < len) {
            if (chars[i] == separatorChar) {
                list.add(new String(chars, start, i - start));
                start = ++i;
                continue;
            }
            i++;
        }
        list.add(str.substring(start, i));
        return list.toArray(EMPTY_STRING_ARRAY);
    }

}
