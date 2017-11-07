package com.eb.bi.rs.frame2.common.storm.datainput;

import com.eb.bi.rs.frame2.common.storm.config.ConfData;
import com.eb.bi.rs.frame2.common.storm.config.ConfRecord;
import com.eb.bi.rs.frame2.common.storm.config.PluginConfig;

public abstract class LoaderBase {
	protected ConfData m_confData = null;
	protected ParserBase m_parser = null;
	public LoaderBase(){}

	public abstract DataRecord getRecord();
	
	public void setConf(ConfData confData){
		this.m_confData = confData;
	}
	public void setParser(ParserBase parser){
		this.m_parser = parser;
	}
	public ParserBase getParser(){
		return m_parser;
	}
	public void init(String name, PluginConfig config){
		confInit(name, config);
		parserInit(name,config);
		connectInit();
		
	}
	private void confInit(String name, PluginConfig config) {
		// TODO Auto-generated method stub
		m_confData = config.getInputConfData(name);
	}
	private boolean parserInit(String name, PluginConfig config){
		if(m_confData == null){
			return false;
		}
		try {//todo:�Ƿ����Ϊ��
			String recordName = config.getInputConfData(name).getMeta();
			System.out.println(recordName);
			ConfRecord record = config.getConfRecord(recordName);
			String parserName = record.getParseType();
			this.setParser(ParserFactory.getParse(parserName));
			m_parser.setConf(record);
			System.out.println(m_parser.m_meta.getParseType());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return true;
	}
	protected abstract boolean connectInit();
}
