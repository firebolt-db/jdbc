package com.firebolt.jdbc.util;

public class StringUtil {
    public static String trim(String s, char t) {
        if (s == null) {
            return null;
        }
        char[] chars = s.toCharArray();
        boolean takeSubstring = false;
        int l = 0;
        for (; l < chars.length; l++) {
            if (chars[l] != t) {
                break;
            }
            takeSubstring = true;
        }
        int r = chars.length - 1;
        for (; r > l; r--) {
            if (chars[r] != t) {
                break;
            }
            takeSubstring = true;
        }
        return takeSubstring ? s.substring(l, r + 1) : s;
    }
}
