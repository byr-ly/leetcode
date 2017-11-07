package com.eb.bi.rs.largeScreen.charge.entity;

import java.util.HashMap;
import java.util.Map;

public class ChargeMap {
	private Map<String , ChargeData> map = new HashMap<String , ChargeData>();

	public Map<String, ChargeData> getMap() {
		return map;
	}

	public void setMap(Map<String, ChargeData> map) {
		this.map = map;
	}
	
}
