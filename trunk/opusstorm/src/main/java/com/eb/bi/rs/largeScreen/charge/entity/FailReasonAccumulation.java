package com.eb.bi.rs.largeScreen.charge.entity;

import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class FailReasonAccumulation extends ChargeData implements Serializable{
	private static final long serialVersionUID = 1L;
	private boolean newData = false;
	private long record_day;
	private long trans_date;
	private Map<String, Long> reason_count = new ConcurrentHashMap<String, Long>();
	private long deal_time;
	
	public FailReasonAccumulation() {
		super();
	}

	public boolean isNewData() {
		return newData;
	}

	public void setNewData(boolean newData) {
		this.newData = newData;
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

	public long getDeal_time() {
		return deal_time;
	}

	public void setDeal_time(long deal_time) {
		this.deal_time = deal_time;
	}
	
	public Map<String, Long> getReason_count() {
		return reason_count;
	}

	public void setReason_count(Map<String, Long> reason_count) {
		this.reason_count = reason_count;
	}

	public void update(long trans_date, long record_day) {		
		if (trans_date > this.trans_date) {
			this.trans_date = trans_date;
			this.record_day = record_day;
		}
		newData = true;
	}
	
	public void add(String error_code, long n){
		Long count = get(error_code);
		count += n;
		reason_count.put(error_code, count);
	}
	
	public void addAll(FailReasonAccumulation fail_acc){
		Map<String, Long> map = fail_acc.getReason_count();
		Set<Entry<String,Long>> entrySet = map.entrySet();
		for (Entry<String, Long> entry : entrySet) {
			String error_code = entry.getKey();
			long count = entry.getValue();
			add(error_code, count);
		}
	}
	
	public Long get(String error_code){
		Long count = reason_count.get(error_code);
		return count == null ? 0 : count;
	}

	public void clear(){
		this.reason_count.clear();
		this.record_day = 0;
		this.trans_date = 0;
		this.deal_time = 0;
	}

}
