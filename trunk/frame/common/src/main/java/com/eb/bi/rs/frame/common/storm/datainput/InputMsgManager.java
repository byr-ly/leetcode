package com.eb.bi.rs.frame.common.storm.datainput;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.eb.bi.rs.frame.common.storm.config.ConfData;
import com.eb.bi.rs.frame.common.storm.config.ConfigReader;
import com.eb.bi.rs.frame.common.storm.config.PluginConfig;

//import backtype.storm.Config;

//import backtype.storm.Config;

public class InputMsgManager {
	private static InputMsgManager m_instance = null;
	private HashMap<String,LoaderBase> m_loaders =new HashMap<String,LoaderBase>();
	public static InputMsgManager getInstance(){
		if(m_instance == null){
			m_instance = new InputMsgManager();
		}
		return m_instance;
	}
	private InputMsgManager(){}
	public void init(PluginConfig config){//��ȡ���ã���ʼ��loader
		ConcurrentHashMap<String, ConfData> confDatas = config.getInputConfDatas();
		Set<Map.Entry<String,ConfData>> set = confDatas.entrySet();
		for(Iterator<Map.Entry<String, ConfData>> it = set.iterator(); it.hasNext();){
			Map.Entry<String, ConfData> entry = (Map.Entry<String, ConfData>)it.next();
			LoaderBase loader = LoaderFactory.getLoader(entry.getValue().getType());
			String name = entry.getKey();
			//loader.setConf(entry.getValue());
			loader.init(name, config);
			m_loaders.put(name, loader);
		}
	}
	public LoaderBase getLoader(String name){
		return m_loaders.get(name);
	}
	public DataRecord getRecord(String name){
		System.out.println(name+"tk");
		LoaderBase loader = m_loaders.get(name);
		if(loader == null){
			return null;
		}
		return loader.getRecord();
		
	}
}
