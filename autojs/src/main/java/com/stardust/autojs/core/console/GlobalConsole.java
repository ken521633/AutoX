package com.stardust.autojs.core.console;

import com.stardust.autojs.core.http.WsManager;
import com.stardust.util.UiHandler;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;


/**
 * Created by Stardust on 2017/10/22.
 */

public class GlobalConsole extends ConsoleImpl {
    public static WsManager LOG_WS_MANAGER = null;
    //public static String IP_DEFAUT = "boot-stg-api.huiliu365.cn";
    public static String IP_DEFAUT = "data-boot-api.huiliu365.com";
    //public static String IP_DEFAUT = "ws-api.tecad.cn";

    public static String DEVICE_ID = "";
    public static String TO_WHO = "/jeecg-boot/websocket/110_";
    public static String TO_LOG = "_log";
    public static String TO_BIZ = "_biz";

    private static final String LOG_tAG = "GlobalConsole";
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("HH:mm:ss.SSS", Locale.getDefault());
    private static final Logger LOGGER = Logger.getLogger(GlobalConsole.class);

    public GlobalConsole(UiHandler uiHandler) {
        super(uiHandler);
    }

    static {
        Date now = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd_hh_mm_ss");
        GlobalConsole.DEVICE_ID = sdf.format(now);
    }

    @Override
    public String println(int level, CharSequence charSequence) {
        String log = String.format(Locale.getDefault(), "%s/%s: %s",
                DATE_FORMAT.format(new Date()), getLevelChar(level), charSequence.toString());
        LOGGER.log(toLog4jLevel(level), log);
        android.util.Log.d(LOG_tAG, log);

        //TODO 将日志写入socket
        if(LOG_WS_MANAGER != null){
            LOG_WS_MANAGER.sendMessage(log);
        }
        super.println(level, log);
        return log;
    }

    private Priority toLog4jLevel(int level) {
        switch (level) {
            case android.util.Log.VERBOSE:
                return Level.DEBUG;
            case android.util.Log.DEBUG:
                return Level.DEBUG;
            case android.util.Log.INFO:
                return Level.INFO;
            case android.util.Log.WARN:
                return Level.WARN;
            case android.util.Log.ERROR:
                return Level.ERROR;
            case android.util.Log.ASSERT:
                return Level.FATAL;
        }
        throw new IllegalArgumentException("invalid level = " + level);
    }

    private String getLevelChar(int level) {
        switch (level) {
            case android.util.Log.VERBOSE:
                return "V";
            case android.util.Log.DEBUG:
                return "D";
            case android.util.Log.INFO:
                return "I";
            case android.util.Log.WARN:
                return "W";
            case android.util.Log.ERROR:
                return "E";
            case android.util.Log.ASSERT:
                return "A";

        }
        return "";
    }

}
