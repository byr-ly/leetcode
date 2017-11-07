package com.eb.bi.rs.mras.bookrec.personalrec.filler;

import java.util.HashMap;
import java.util.Map;

public class BookStores {
	//key:class_id;val:book_id,real_fee
	private Map<String,BookStore> m_BookMap = new HashMap<String,BookStore>();

	public BookStores() {
		// TODO Auto-generated constructor stub
	}

	public void add(String class_id,String book_id,String real_fee){
		if(m_BookMap.containsKey(class_id)){
			
		}
		else{
		//	BookStore bookstore = new BookStore();
		//	bookstore.add(book_id, real_fee);
		//	m_BookMap.put(class_id, bookstore);
		}
	}
}
