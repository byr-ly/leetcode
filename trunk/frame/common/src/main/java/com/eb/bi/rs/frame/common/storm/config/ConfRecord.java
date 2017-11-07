package com.eb.bi.rs.frame.common.storm.config;

import java.util.concurrent.ConcurrentHashMap;

public class ConfRecord {

	private String parseType;
	private String fieldDelimiter;
	private String recordDelimiter;
	private ConcurrentHashMap<String, ConfRecordField> fieldMap = new ConcurrentHashMap<String, ConfRecordField>();

	public ConfRecord() {
		// TODO Auto-generated constructor stub
	}

	public ConfRecord(String parseType, String fieldDelimiter,
			String recordDelimiter) {
		this.parseType = parseType;
		this.fieldDelimiter = fieldDelimiter;
		this.recordDelimiter = recordDelimiter;
	}

	public String getParseType() {
		return parseType;
	}

	public void setParseType(String parseType) {
		this.parseType = parseType;
	}

	public String getFieldDelimiter() {
		return fieldDelimiter;
	}

	public void setFieldDelimiter(String fieldDelimiter) {
		this.fieldDelimiter = fieldDelimiter;
	}

	public String getRecordDelimiter() {
		return recordDelimiter;
	}

	public void setRecordDelimiter(String recordDelimiter) {
		this.recordDelimiter = recordDelimiter;
	}

	public ConcurrentHashMap<String, ConfRecordField> getFieldMap() {
		return fieldMap;
	}

	public void setFieldMap(ConcurrentHashMap<String, ConfRecordField> fmap) {
		fieldMap = fmap;
	}

	public ConfRecordField getField(String fieldName) {
		return fieldMap.get(fieldName);
	}

	public ConfRecordField putField(String fieldName,
			ConfRecordField confRecordField) {
		return fieldMap.put(fieldName, confRecordField);
	}

}
