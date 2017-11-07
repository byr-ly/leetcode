package com.eb.bi.rs.mras2.bookrec.guessyoulike.util;

import java.util.HashMap;
import java.util.Map;

public class Similars {
	//后图书，与前图书的相似度向量
	private Map<String,Similar> book_Similars = new HashMap<String,Similar>();

	public void add(String bookid, String similarString, String separator){
		Similar oneSimilar = new Similar();
		oneSimilar.init(similarString,separator);
		book_Similars.put(bookid, oneSimilar);
	}

	public Map<String,Similar> getSimilars(){
		return book_Similars;
	}
}
