package com.eb.bi.rs.frame.common.storm.config;

import java.util.concurrent.ConcurrentHashMap;

public class ConfDataList {
	private ConcurrentHashMap<String, ConfData> inConfDatas = new ConcurrentHashMap<String, ConfData>();
	private ConcurrentHashMap<String, ConfData> outConfDatas = new ConcurrentHashMap<String, ConfData>();
	
	public ConcurrentHashMap<String, ConfData> getInConfDatas(){
		return inConfDatas;
	}
	public ConcurrentHashMap<String, ConfData> getOutConfDatas(){
		return outConfDatas;
	}
	public void putInConfData(String name, ConfData data){
		inConfDatas.put(name, data);
	}
	public void putOutConfData(String name, ConfData data){
		outConfDatas.put(name, data);
	}
}
