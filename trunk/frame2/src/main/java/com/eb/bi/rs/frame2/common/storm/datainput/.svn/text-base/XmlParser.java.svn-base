package com.eb.bi.rs.frame2.common.storm.datainput;

import com.eb.bi.rs.frame2.common.storm.config.ConfRecord;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

import java.util.Iterator;


public class XmlParser extends ParserBase {
	public XmlParser(){}
	public XmlParser(ConfRecord meta){
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
