package com.eb.bi.rs.algorithm.pac.Util;

public class PrintUtil {
    private static LogUtil logUtil = LogUtil.getInstance();

    public static void printArray(double[] array, String title, boolean bRows) {
        title = String.format("%s--------------", new Object[]{title});
        logUtil.getLogger().info(title);
        if (bRows) {
            for (int i = 0; i < array.length; ++i) {
                logUtil.getLogger().info(Double.valueOf(array[i]));
            }
        } else {
            StringBuffer buffer = new StringBuffer();
            for (int i = 0; i < array.length; ++i) {
                buffer.append(array[i]);
                buffer.append(" ");
            }
            logUtil.getLogger().info(buffer.toString());
        }
        logUtil.getLogger().info("\n");
    }

    public static void printArray(double[][] array, String title) {
        title = String.format("%s--------------", new Object[]{title});
        logUtil.getLogger().info(title);
        for (int i = 0; i < array.length; ++i) {
            StringBuffer buffer = new StringBuffer();
            for (int j = 0; j < array[0].length; ++j) {
                buffer.append(array[i][j]);
                buffer.append(" ");
            }
            logUtil.getLogger().info(buffer.toString());
        }
        logUtil.getLogger().info("\n");
    }

    public static void printLog(String str) {
        logUtil.getLogger().info(str);
    }
}