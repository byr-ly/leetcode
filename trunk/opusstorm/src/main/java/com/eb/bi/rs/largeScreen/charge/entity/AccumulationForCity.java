package com.eb.bi.rs.largeScreen.charge.entity;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class AccumulationForCity extends ChargeData implements Serializable{
	private static final long serialVersionUID = 1L;
	private boolean newData = false;
	private long record_day;
	private long trans_date;
	private Map<String, Double> city_charge = new ConcurrentHashMap<String, Double>();
	private Map<String, Long> city_count = new ConcurrentHashMap<String, Long>();
	private Map<String, Set<String>> city_uv = new ConcurrentHashMap<String, Set<String>>();

	public AccumulationForCity() {
		super();
	}

	public boolean isNewData() {
		return newData;
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
	
	public Map<String, Long> getCity_count() {
		return city_count;
	}

	public void setCity_count(Map<String, Long> city_count) {
		this.city_count = city_count;
	}

	public Map<String, Set<String>> getCity_uv() {
		return city_uv;
	}

	public void setCity_uv(Map<String, Set<String>> city_uv) {
		this.city_uv = city_uv;
	}

	public Map<String, Double> getCity_charge() {
		return city_charge;
	}

	public void setCity_charge(Map<String, Double> city_charge) {
		this.city_charge = city_charge;
	}

	public void addCharge(String city_id, Double price){
		Double charge = getCharge(city_id);
		charge += price;
		city_charge.put(city_id, charge);
	}
	
	public Double getCharge(String city_id){
		Double charge = city_charge.get(city_id);
		return charge == null ? 0.0 : charge;
	}
	
	public void addCount(String city_id, long cnt){
		Long count = getCount(city_id);
		count += cnt;
		city_count.put(city_id, count);
	}
	
	public Long getCount(String city_id){
		Long count = city_count.get(city_id);
		return count == null ? 0 : count;
	}
	
	public void addMsisdn(String city_id, String msisdn){
		Set<String> msisdnSet = getMsisdnSet(city_id);
		msisdnSet.add(msisdn);
	}
	
	public void addAllMsisdn(String city_id, Set<String> set){
		Set<String> msisdnSet = getMsisdnSet(city_id);
		msisdnSet.addAll(set);
	}
	
	private Set<String> getMsisdnSet(String city_id) {
		Set<String> set = city_uv.get(city_id);
		if (set == null) {
			set = new HashSet<String>();
			city_uv.put(city_id, set);
		}
		return set;
	}
	
	public void add(String city_id, Double price, long cnt, String msisdn){
		addCharge(city_id, price);
		addCount(city_id, cnt);
		addMsisdn(city_id, msisdn);
	}
	
	public void addAll(AccumulationForCity acc_city){
		Map<String, Double> charge_map = acc_city.getCity_charge();
		Map<String, Long> count_map = acc_city.getCity_count();
		Map<String, Set<String>> uv_map = acc_city.getCity_uv();
		Set<String> keySet = charge_map.keySet();
		for (String key : keySet) {
			addCharge(key, charge_map.get(key));
			addCount(key, count_map.get(key));
			addAllMsisdn(key, uv_map.get(key));
		}
		
	}
	
	public void update(long trans_time, long record_day){
		if (this.trans_date < trans_time) {
			this.trans_date = trans_time;
			this.record_day = record_day;
		}
		newData = true;
	}
	
	public void clear(){
		this.record_day = 0;
		this.trans_date = 0;
		city_charge.clear();
		city_count.clear();
		Set<Entry<String,Set<String>>> uv_entrySet = city_uv.entrySet();
		for (Entry<String, Set<String>> entry : uv_entrySet) {
			entry.getValue().clear();
		}
		city_uv.clear();
	}
}
