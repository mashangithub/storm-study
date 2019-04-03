package com.aura.bigdata.storm.util;

import java.text.SimpleDateFormat;
import java.util.Date;

public class MyStormUtil {

    public static String dateFormat() {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
    }

    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
