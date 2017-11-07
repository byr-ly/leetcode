package com.eb.bi.rs.frame2.common.storm.datainput;

import com.eb.bi.rs.frame2.common.storm.config.ConfRecord;

/**
 * ��ݽ�������
 * @author 
 * @date 
 * @since 
 */

public abstract class ParserBase {
	protected ConfRecord m_meta;
	protected int correctRecord=0;
	protected int badRecord=0;
	
	public ParserBase(){}
	public ParserBase(ConfRecord meta){
		this.m_meta = meta;
	}
	public void setConf(ConfRecord meta){
		this.m_meta = meta;
	}
	public boolean doParse(String text, DataRecord record){return true;}
	
	//public abstract boolean doParse(String text, DataRecord record);
	public void print(){}
	public int getCorrectRecord(){
		return correctRecord;
	}
	
	public int getBadRecord(){
		return badRecord;
	}
}
