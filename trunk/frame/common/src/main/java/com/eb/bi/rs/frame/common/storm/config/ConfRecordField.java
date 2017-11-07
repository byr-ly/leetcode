package com.eb.bi.rs.frame.common.storm.config;

public class ConfRecordField {

	private int index;
	private boolean nullable;
	private int size;
	private String type;

	public ConfRecordField() {
		// TODO Auto-generated constructor stub
	}

	public ConfRecordField(int index, boolean nullable, int size, String type) {
		this.index = index;
		this.nullable = nullable;
		this.size = size;
		this.type = type;
	}
	
	public int getIndex() {
		return index;
	}
	
	public void setIndex(int index) {
		this.index = index;
	}
	
	public boolean getNullable() {
		return nullable;
	}
	
	public void setNullable(boolean nullable) {
		this.nullable = nullable;
	}
	
	public int getSize() {
		return size;
	}
	
	public void setSize(int size) {
		this.size = size;
	}
	
	public String getType() {
		return type;
	}
	
	public void setType(String type) {
		this.type = type;
	}

}
