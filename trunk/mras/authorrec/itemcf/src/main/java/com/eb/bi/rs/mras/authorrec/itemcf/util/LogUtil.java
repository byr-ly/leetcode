package com.eb.bi.rs.mras.authorrec.itemcf.util;


import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class LogUtil {

    private Logger pluginLogger = null;
    private static LogUtil instance = null;

    public static LogUtil getInstance() {
        if (instance == null) {
            instance = new LogUtil();
        }
        return instance;
    }

    private LogUtil() {
    }

    public Logger getLogger() {
        return pluginLogger;
    }

    public boolean initLog(String logConfigFilePath, String logCategory) {
        PropertyConfigurator.configure(logConfigFilePath);
        pluginLogger = Logger.getLogger(logCategory);
        if (pluginLogger == null) {
            System.out.println("ERROR: Initialize log failed.");
            return false;
        } else {
            pluginLogger.info("begin match\n==============================");
        }
        return true;
    }
}
