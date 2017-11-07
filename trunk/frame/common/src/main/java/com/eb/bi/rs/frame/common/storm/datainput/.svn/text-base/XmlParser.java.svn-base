package com.eb.bi.rs.frame.common.storm.datainput;
import java.util.Iterator;

import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

import com.eb.bi.rs.frame.common.storm.config.ConfRecord;


public class XmlParser extends ParserBase {
	public XmlParser(){}
	public XmlParser(com.eb.bi.rs.frame.common.storm.config.ConfRecord meta){
		super(meta);
	}
	public boolean doParse(String text, DataRecord record){
		
		Document doc = null;
		try{
			doc = DocumentHelper.parseText(text);
			Element rootElt = doc.getRootElement();
			Iterator it = rootElt.elementIterator();
			
			while(it.hasNext()){
				Element param = (Element)it.next();
				DataField dataField= new DataField(param.getText());
				record.addField(dataField);
			}
		}
		catch(Exception e){
			e.printStackTrace();
			return false;
		}
		return true;
	}
	public void print(){
		System.out.println("i am XmlParser");
	}
}
