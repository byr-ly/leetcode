package com.eb.bi.rs.mrasstorm.unifyrec.realtimerec.domain;

import java.util.HashSet;

/*
 * Hbase表'unified_rec_rep'
 * 
 * @author ynn
 * @date 创建时间：2015-11-23 上午11:00:50
 * @version 1.0
 */
public class RecRepBookInfo {
	private String tag;
	private HashSet<Integer> editions;
	public RecRepBookInfo() {
	}
	
	public RecRepBookInfo(String tag, String editions) {
		this.tag = tag;	
		this.editions = new HashSet<Integer>();
		for (String edition :  editions.split(",")) {			
			this.editions.add(Integer.parseInt(edition));
		}		
	}

	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}

	public HashSet<Integer> getEditions() {
		return editions;
	}

	public void setEditions(HashSet<Integer> editions) {
		this.editions = editions;
	}

}
