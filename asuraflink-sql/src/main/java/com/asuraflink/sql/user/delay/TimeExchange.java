package com.asuraflink.sql.user.delay;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class TimeExchange {

    /**
     * 13位int型的时间戳转换为String(yyyy-MM-dd HH:mm:ss)
     * @param time
     * @return
     */
    public static String timestampToString(long time){
        //int转long时，先进行转型再进行计算，否则会是计算结束后在转型
        Timestamp ts = new Timestamp(time);
        String tsStr = "";
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            tsStr = dateFormat.format(ts);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return tsStr;
    }

}
