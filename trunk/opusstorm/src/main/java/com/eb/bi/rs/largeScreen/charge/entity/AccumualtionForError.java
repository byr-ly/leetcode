package com.eb.bi.rs.largeScreen.charge.entity;

import java.io.Serializable;

public class AccumualtionForError implements Serializable{
	private static final long serialVersionUID = 1L;
	private long record_day;
	private long trans_date;
	private String error_code;
	private String error_code_name;
	private long trans_cnt;
	
	public AccumualtionForError() {
		super();
	}

	public AccumualtionForError(long record_day, long trans_date,
			String error_code, String error_code_name, long trans_cnt) {
		super();
		this.record_day = record_day;
		this.trans_date = trans_date;
		this.error_code = error_code;
		this.error_code_name = error_code_name;
		this.trans_cnt = trans_cnt;
	}

	public long getRecord_day() {
		return record_day;
	}

	public void setRecord_day(long record_day) {
		this.record_day = record_day;
	}

	public long getTrans_date() {
		return trans_date;
	}

	public void setTrans_date(long trans_date) {
		this.trans_date = trans_date;
	}

	public String getError_code() {
		return error_code;
	}

	public void setError_code(String error_code) {
		this.error_code = error_code;
	}

	public String getError_code_name() {
		return error_code_name;
	}

	public void setError_code_name(String error_code_name) {
		this.error_code_name = error_code_name;
	}

	public long getTrans_cnt() {
		return trans_cnt;
	}

	public void setTrans_cnt(long trans_cnt) {
		this.trans_cnt = trans_cnt;
	}
	
}
