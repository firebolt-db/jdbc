package com.firebolt.jdbc.util;

import lombok.experimental.UtilityClass;

import static java.lang.String.format;

@UtilityClass
public class ByteArrayUtil {
    public static byte[] hexStringToByteArray(String s) {
        char[] chars = s.toCharArray();
        int n = chars.length;
        byte[] data = new byte[n / 2];
        for (int i = 0; i < n; i += 2) {
            data[i / 2] = (byte) ((hexDigit(chars[i]) << 4) + hexDigit(chars[i + 1]));
        }
        return data;
    }

    private int hexDigit(char c) {
        int d = Character.digit(c, 16);
        if (d < 0) {
            throw new IllegalArgumentException(format("Illegal character %s in hex string", c));
        }
        return d;
    }
}
