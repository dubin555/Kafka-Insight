package common.util;

import static org.junit.Assert.*;

/**
 * Created by dubin on 30/09/2017.
 */
public class StrUtilsTest {
    @org.junit.Test
    public void stringify() throws Exception {
        assertEquals(StrUtils.stringify(1000), "1000B");
        assertEquals(StrUtils.stringify(1024L), "1.00KB");
        assertEquals(StrUtils.stringify(1024*1024L), "1.00MB");
        assertEquals(StrUtils.stringify(1024*1024*1024L), "1.00GB");
        assertEquals(StrUtils.stringify(1024*1024*1024*1024L), "1.00TB");
    }

    @org.junit.Test
    public void numberic() throws Exception {
        assertEquals(StrUtils.numberic("1"), 1.00, 0.000000001);
        assertNotEquals(StrUtils.numberic("1"), 0.99, 0.000000001);
        assertEquals(StrUtils.numberic("1.099"), 1.10, 0.000000001);
    }

    @org.junit.Test
    public void integer() throws Exception {
        assertEquals(StrUtils.integer(1.01), 1);
    }

}