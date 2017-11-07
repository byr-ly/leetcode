package com.eb.bi.rs.mras2.bookrec.knowledgecal.bookmark;

import java.util.HashMap;
import java.util.Map;

import com.eb.bi.rs.mras2.bookrec.knowledgecal.bookmark.Bookinfo;

/**
 * @author ynn
 * @date 创建时间：2015-12-25 下午3:04:36
 * @version 1.0
 */
public class BooksInfo {

	private Map<String, Bookinfo> booktypeMap = new HashMap<String, Bookinfo>();

	public void add(String booId, String Type, String readNum) {
		booktypeMap.put(booId, new Bookinfo(Type, readNum));
	}

	public int getBooktype(String booId) {
		if (!booktypeMap.containsKey(booId))
			return -1;

		return booktypeMap.get(booId).getType();
	}

	public int getBookreadnum(String booId) {
		if (!booktypeMap.containsKey(booId))
			return -1;
		return booktypeMap.get(booId).getReadnum();
	}
}
