package org.kettle.beam.core.util;

import java.math.BigDecimal;

public class Strings {

    public static boolean isNullOrEmpty(String value) {
        return value == null || value.isEmpty() || value.trim() == "";
    }

    public static String paddingLeft(String value, String character, int length) {
        if(length <= 0) {return value;}
        if(value.length() >= length) {return value;}
        StringBuilder builder = new StringBuilder();
        builder.append(value);
        while(builder.length() < length) {
            builder.insert(0, character);
        }
        value = builder.toString();
        return value.length() <= length ? value : value.substring(value.length() - length);
    }

    public static String paddingRight(String value, String character, int length) {
        if(length <= 0) {return value;}
        if(value.length() >= length) {return value;}
        StringBuilder builder = new StringBuilder();
        builder.append(value);
        while(builder.length() < length) {
            builder.append(character);
        }
        value = builder.toString();
        return value.length() <= length ? value : value.substring(0, length);
    }

    public static Object convert(String value, Class<?> type) throws Exception {
        Object objValue = null;

        if(type == null || type.equals(Object.class)) {return objValue;}
        if(Strings.isNullOrEmpty(value)) {return objValue;}

        if(type.equals(String.class)) {
            objValue = value;

        }else if(type.equals(Boolean.class)) {
            objValue = "true".equalsIgnoreCase(value) || "1".equals(value);

        }else if(type.equals(Long.class)) {
            objValue = Long.parseLong(value);

        }else if(type.equals(Integer.class)) {
            objValue = Integer.parseInt(value);

        }else if(type.equals(Short.class)) {
            objValue = Short.parseShort(value);

        }else if(type.equals(Double.class)) {
            objValue = Double.parseDouble(value);

        }else if(type.equals(Float.class)) {
            objValue = Float.parseFloat(value);

        }else if(type.equals(BigDecimal.class)) {
            objValue = new BigDecimal(value);

        }else {
            objValue = value;
        }

        return objValue;
    }

    public static byte[] toByte(String hexadecimal) {
        int len = hexadecimal.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hexadecimal.charAt(i), 16) << 4)
                    + Character.digit(hexadecimal.charAt(i + 1), 16));
        }
        return data;
    }

    public static String toHexadecimal(byte[] buffer) {
        String hex = "";
        String h = "0123456789ABCDEF";
        for (int i = 0; i < buffer.length; i++) {
            int v = buffer[i] & 0xff;
            hex += h.charAt(v >>> 4);
            hex += h.charAt(v & 15);
        }
        return hex;
    }

}
