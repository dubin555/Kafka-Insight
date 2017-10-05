package common.util;

import java.text.DecimalFormat;

/**
 * Created by dubin on 29/09/2017.
 */
public class StrUtils {

    private final static long KB_IN_BYTES = 1024;

    private final static long MB_IN_BYTES = 1024 * KB_IN_BYTES;

    private final static long GB_IN_BYTES = 1024 * MB_IN_BYTES;

    private final static long TB_IN_BYTES = 1024 * GB_IN_BYTES;

    private final static DecimalFormat df = new DecimalFormat("0.00");

    private static String SYSTEM_ENCODING = System.getProperty("file.encoding");

    static {
        if (SYSTEM_ENCODING == null) {
            SYSTEM_ENCODING = "UTF-8";
        }
    }

    private StrUtils() {
    }

    /** Formatter byte to kb,mb or gb etc. */
    public static String stringify(long byteNumber) {
        if (byteNumber / TB_IN_BYTES > 0) {
            return df.format((double) byteNumber / (double) TB_IN_BYTES) + "TB";
        } else if (byteNumber / GB_IN_BYTES > 0) {
            return df.format((double) byteNumber / (double) GB_IN_BYTES) + "GB";
        } else if (byteNumber / MB_IN_BYTES > 0) {
            return df.format((double) byteNumber / (double) MB_IN_BYTES) + "MB";
        } else if (byteNumber / KB_IN_BYTES > 0) {
            return df.format((double) byteNumber / (double) KB_IN_BYTES) + "KB";
        } else {
            return String.valueOf(byteNumber) + "B";
        }
    }

    /** Formmatter number. */
    public static double numberic(String number) {
        DecimalFormat formatter = new DecimalFormat("###.##");
        return Double.valueOf(formatter.format(Double.valueOf(number)));
    }

    public static double numberic(Double number) {
        DecimalFormat formatter = new DecimalFormat("###.##");
        return Double.valueOf(formatter.format(number));

    }

    /** Convert string number to double. */
    public static long integer(double number) {
        return Math.round(number);
    }

    /** Assembly number to string. */
    public static String assembly(String number) {
        return stringify(integer(numberic(number)));
    }

    public static String assembly(Double number) {
        return stringify(integer(numberic(number)));
    }

}
