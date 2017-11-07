package com.eb.bi.rs.frame2.common.storm.config;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;


public class PluginConfig {

    private Properties params = new Properties();
    private ConcurrentHashMap<String, ConfData> inConfDatas = new ConcurrentHashMap<String, ConfData>();
    private ConcurrentHashMap<String, ConfData> outConfDatas = new ConcurrentHashMap<String, ConfData>();
    private ConcurrentHashMap<String, ConfRecord> confRecords = new ConcurrentHashMap<String, ConfRecord>();

    public ConcurrentHashMap<String, ConfData> getInputConfDatas() {
        return inConfDatas;
    }

    public ConcurrentHashMap<String, ConfData> getOutputConfDatas() {
        return outConfDatas;
    }

    public boolean inputConfDataExists(String dataName) {
        return inConfDatas.containsKey(dataName);
    }

    public boolean outputConfDataExists(String dataName) {
        return outConfDatas.containsKey(dataName);
    }

    public ConfData getInputConfData(String dataName) {
        return inConfDatas.get(dataName);
    }

    public ConfData putInConfData(String dataName, ConfData confData) {
        return inConfDatas.put(dataName, confData);
    }

    public ConfData putOutConfData(String dataName, ConfData confData) {
        return outConfDatas.put(dataName, confData);
    }

    public boolean paramExists(String paramName) {
        return params.containsKey(paramName);
    }

    public Properties getParams() {
        return params;
    }

    public String getParam(String paramName) {
        return params.getProperty(paramName);
    }

    public Object putParam(String paramName, String paramValue) {
        return params.setProperty(paramName, paramValue);
    }

    public String paramToString() {
        return "Param List:" + params.toString();
    }

    public ConcurrentHashMap<String, ConfRecord> getConfRecords() {
        return confRecords;
    }

    public boolean confRecordExists(String recordName) {
        return confRecords.containsKey(recordName);
    }

    public ConfRecord getConfRecord(String recordName) {
        return confRecords.get(recordName);
    }

    public ConfRecord putConfRecord(String recordName, ConfRecord confRecord) {
        return confRecords.put(recordName, confRecord);
    }
}
