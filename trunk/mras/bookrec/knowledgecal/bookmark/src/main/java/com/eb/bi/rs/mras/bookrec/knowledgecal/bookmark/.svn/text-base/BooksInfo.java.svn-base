package com.eb.bi.rs.mras.bookrec.knowledgecal.bookmark;

import java.util.HashMap;
import java.util.Map;

public class BooksInfo {
	private Map<String,Bookinfo> booktypeMap = new HashMap<String,Bookinfo>();
	
	public void add(String booId, String Type, String readNum){
		booktypeMap.put(booId, new Bookinfo(Type,readNum));
	}
	
	public int getBooktype(String booId){
		if(!booktypeMap.containsKey(booId))
			return -1;
		
		return booktypeMap.get(booId).getType();
	}
	
	public int getBookreadnum(String booId){
		if(!booktypeMap.containsKey(booId))
			return -1;
		return booktypeMap.get(booId).getReadnum();
	}
}
