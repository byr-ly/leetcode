package com.eb.bi.rs.algorithm.pac.Util;

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

    public Logger getLogger() {
        return this.pluginLogger;
    }

    public boolean initLog(String logConfigFilePath, String logCategory) {
        PropertyConfigurator.configure(logConfigFilePath);
        this.pluginLogger = Logger.getLogger(logCategory);
        if (this.pluginLogger == null) {
            System.out.println("ERROR: Initialize log failed.");
            return false;
        }

        this.pluginLogger.info("begin pca.PCACalcu\n==============================");

        return true;
    }
}