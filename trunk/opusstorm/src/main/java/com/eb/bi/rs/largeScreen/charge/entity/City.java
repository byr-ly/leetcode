package com.eb.bi.rs.largeScreen.charge.entity;

import java.io.Serializable;

public class City implements Serializable{
	private static final long serialVersionUID = 1L;
	private String prv_id;
	private String prv_name;
	private String city_id;
	private String city_name;
	
	public City() {
		super();
	}

	public void initialize(String prv_id, String prv_name, String city_id, String city_name) {
		this.prv_id = prv_id;
		this.prv_name = prv_name;
		this.city_id = city_id;
		this.city_name = city_name;
	}

	public String getPrv_id() {
		return prv_id;
	}

	public void setPrv_id(String prv_id) {
		this.prv_id = prv_id;
	}

	public String getPrv_name() {
		return prv_name;
	}

	public void setPrv_name(String prv_name) {
		this.prv_name = prv_name;
	}

	public String getCity_id() {
		return city_id;
	}

	public void setCity_id(String city_id) {
		this.city_id = city_id;
	}

	public String getCity_name() {
		return city_name;
	}

	public void setCity_name(String city_name) {
		this.city_name = city_name;
	}
	
}
