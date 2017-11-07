package com.eb.bi.rs.algorithm.pac.Util;

public class ArrayCalcuUtil {
    public static int[] sortArray(double[] values, double[] orderedValues) {
        if (values.length < 1) {
            return null;
        }
        int[] orderPos = new int[values.length];
        orderedValues[0] = values[0];
        orderPos[0] = 0;
        for (int i = 1; i < values.length; ++i) {
            double value = values[i];
            int pos = i;
            for (int j = i - 1; j >= 0; --j) {
                if (value <= orderedValues[j])
                    break;
            }

            for (int k = i - 1; k >= pos; --k) {
                orderedValues[(k + 1)] = orderedValues[k];
                orderPos[(k + 1)] = orderPos[k];
            }
            orderedValues[pos] = value;
            orderPos[pos] = i;
        }
        return orderPos;
    }
}