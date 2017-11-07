package com.eb.bi.rs.mras2.bookrec.personalrec.filler;

public class BookStore {
	private String m_key;
	private String m_text;
	private float sort_val;
	
	BookStore(String k,String t,float b){
		m_key=k;
		m_text=t;
		sort_val=b;
	}
	
	public String toString(){
		return m_text;
	}
	
	public String getKey(){
		return m_key;
	}
	
	public String getText(){
		return m_text;
	}
}
