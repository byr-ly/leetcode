package com.eb.bi.rs.mras.authorrec.itemcf.thread;

import java.util.ArrayList;
import java.util.List;

public class CollParalleMonitor {

    private List<PredictScoreThread> monitors = null;
    private int failureNum = 0;
    private int successNum = 0;
    private String resultDec = null;

    public CollParalleMonitor() {
        monitors = new ArrayList<PredictScoreThread>();
        failureNum = 0;
        successNum = 0;
    }

    public void monitor(PredictScoreThread worker) {
        monitors.add(worker);
    }

    public void statistic() {
        StringBuffer buffer = new StringBuffer();
        buffer.append("PredictScoreThread results: ");
        for (PredictScoreThread m : monitors) {
            if (m.isSuccess()) {
                successNum++;
            } else {
                failureNum++;
                buffer.append(String.format("thread %d failed.\t", m.getThreadId()));
            }
        }
        resultDec = buffer.toString();
    }

    public int getFailureNum() {
        return failureNum;
    }

    public int getSuccessNum() {
        return successNum;
    }

    public String getResultDec() {
        return resultDec;
    }
}
