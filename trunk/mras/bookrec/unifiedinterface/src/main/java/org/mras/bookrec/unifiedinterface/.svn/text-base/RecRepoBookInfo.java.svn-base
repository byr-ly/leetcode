package org.mras.bookrec.unifiedinterface;

import java.util.HashSet;

public class RecRepoBookInfo {
	private String tag;
	private HashSet<Integer> editions;
	public RecRepoBookInfo() {
	}
	
	public RecRepoBookInfo(String tag, String editions) {
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
