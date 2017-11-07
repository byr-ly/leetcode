package com.eb.bi.rs.mras.seachrec.relatedkey.online;



public enum DataList {

	DM_USER_LIST_SET("user_list"),
	DIM_BOOK_NAME_RE_ID_SET("book_list"),
	DIM_AUTHOR_NAME_RE_ID_SET("author_list"),
	DM_BOOKID_TAG_WEIGHT_HASH("book_tags"),
	DM_AUTHORID_TAG_WEIGHT_HASH("author_tags"),
	DM_SEARCHWORD_TAG_WEIGHT_HASH("word_tags"),
	DM_KEY_RELATED_WEIGHT_HASH("key_related");	
	
	private String name;
	private DataList(String name){
		setName(name);
	}
	public String getName() {
		return name;
	}
	public void setName(String name){
		this.name = name;
	}	
}