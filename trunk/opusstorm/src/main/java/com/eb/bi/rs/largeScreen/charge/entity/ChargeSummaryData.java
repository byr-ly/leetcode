package com.eb.bi.rs.largeScreen.charge.entity;

import java.io.Serializable;

public class ChargeSummaryData extends ChargeData implements Serializable{
	private static final long serialVersionUID = 1L;
	private long record_day = 0;
	private long trans_date = 0;
	private long trans_cnt = 0;
	private long deal_time = 0;
	 
	public ChargeSummaryData() {
		super();
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

	public long getTrans_cnt() {
		return trans_cnt;
	}

	public void setTrans_cnt(long trans_cnt) {
		this.trans_cnt = trans_cnt;
	}

	public long getDeal_time() {
		return deal_time;
	}

	public void setDeal_time(long deal_time) {
		this.deal_time = deal_time;
	}
	//检验日期数据是否正确，并更新交易时间
	public void update(long n, long trans_date, long record_day){
		this.trans_cnt += n;
		if (trans_date > this.trans_date) {
			this.trans_date = trans_date;
			this.record_day = record_day;
		}
	}
	
	public void clear(){
		this.deal_time=0;
		this.record_day=0;
		this.trans_cnt=0;
		this.trans_date=0;
	}
	 
}
