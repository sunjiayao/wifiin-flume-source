package com.wifiin.flume.source;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @auther sunjiayao on 16/5/14.
 * email: sunjiayaocode@gmail.com
 */
public class WifiinTailFileConfigurationConstants {
    //需要读取的文件名
    public static final String FILE_NAME = "file";
    public static final String FILE_NAME_DEFAULT = new SimpleDateFormat("yyyy-MM-dd").format(new Date())+".log";

    //需要读取的目录
    public static final String PATH = "path";
    public static final String PATH_DEFAULT = "/";


    public static final String OFFSET_FILE_PATH = "offset_path";
    public static final String OFFSET_FILE_PATH_DEFAULT = "";


    public static final String EVENT_SIZE = "event_size";
    public static final int EVENT_SIZE_DEFAULT=1000;

    public static final String FILE_LINES_SIZE = "file_lines_size";
    public static final int FILE_LINES_SIZE_DEFAULT = 100_000;

    public static final String FILE_CHARSET="charset";
    public static final String FILE_CHARSET_DEFAULT="UTF8";
}
