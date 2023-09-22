package org.zarka.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Formatter {
    
    public static String getFileNameExtension() {
        Date date = new Date(System.currentTimeMillis());
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
        return dateFormat.format(date);
    }
}
