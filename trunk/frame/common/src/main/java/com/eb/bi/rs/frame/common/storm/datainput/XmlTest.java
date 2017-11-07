package com.eb.bi.rs.frame.common.storm.datainput;
import java.util.Iterator;
import java.util.Vector;

import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;


public class XmlTest {
public static boolean doParse(String text){
		Vector<String> params = new Vector<String>();
		Document doc = null;
		try{
			doc = DocumentHelper.parseText(text);
			Element rootElt = doc.getRootElement();
			Iterator it = rootElt.elementIterator();
			while(it.hasNext()){
				System.out.println(it.hasNext());
				Element param = (Element)it.next();
				System.out.println("param= "+param.getName());
				params.add(param.getText());
				
			}
			System.out.println(params);
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return true;
	}
	public static void main(String[] args){
		String str="<xml>"+"<data1>xyz</data1>"+"<data2>abc</data2>"+"</xml>";
		XmlTest.doParse(str);
		return;
	}
}
