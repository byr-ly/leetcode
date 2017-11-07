package com.eb.bi.rs.mras.bookrec.knowledgecal.bookmark;

public class Bookinfo {
	private int TYPE;
	private int readNum;
	
	Bookinfo(String TYPE, String readNum){
		this.TYPE= Integer.valueOf(TYPE);
		this.readNum= Integer.valueOf(readNum);
	}
	
	public int getType(){
		return TYPE;
	}
	
	public int getReadnum(){
		return readNum;
	}
}
