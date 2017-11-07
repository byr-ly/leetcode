package com.eb.bi.rs.mras.seachrec.keyseach.online;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
//import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
//import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;

public class PrefCompute {
//	private Logger m_logger;
	
	private String m_user;
	private String m_word;
	private String m_type;
	private String m_tagstrKey;
	private String m_tagstrVal;
	private int m_number;
	
	private String[] m_tagsKeys;
	private double[] m_tagsVals;
	private Set<String> m_bookId = new HashSet<String>();
	
	private SortedSet<BookPrefData> m_book_weight = new TreeSet<BookPrefData>();
	
	private ArrayList<String> m_bookid;
	
	public PrefCompute(String user,String word,String type,String tagstr_key,String tagstr_val, int number, Logger logger){
		m_user = user;
		m_word = word;
		m_type = type;
		m_tagstrKey = tagstr_key;
		m_tagstrVal = tagstr_val;
		m_number = number;
//		m_logger = logger;
	}
	
	private void splittags(){
		//String m_tagstr*处理
		m_tagsKeys = m_tagstrKey.split(", ");
		
		m_tagsVals = new double[m_tagsKeys.length];
		
		String[] val_tmp = m_tagstrVal.split(", ");
		
		for(int i = 0; i != val_tmp.length; i++){
			m_tagsVals[i] = Double.valueOf(val_tmp[i]).doubleValue();
		}
	}
	
	public void compute(Jedis m_redis, int m_prefcom_flag){
		splittags();
		
		for(int j =0; j != m_tagsKeys.length; j++){
			m_bookId.addAll(m_redis.smembers(DataList.dim_tag_re_recomendbooks_set + ":" + m_tagsKeys[j]));
		}
		
//		m_logger.debug("PrefCompute book_list is : "+m_bookId.toString());//test
		
		if(m_bookId == null){
			return;
		}
		
		Iterator<String> it = m_bookId.iterator();
		
		while(it.hasNext()){
			//删除历史已读书//目前没定用什么存
			String bookid = it.next();
			if(m_redis.sismember(DataList.dm_user_his_book_set + ":" + m_user, bookid)){
				it.remove();
				continue;
			}
			else {
				//判断搜索词是否是图书名
				if(m_type.equals("0")){
					if(m_redis.sismember(DataList.dim_book_name_re_id_set + ":" + m_word, bookid))
						it.remove();
					continue;
				}
				//判断搜索词是否是作者
				if(m_type.equals("1")){
					for(String authorId : m_redis.smembers(DataList.dim_author_name_re_id_set + ":" + m_word)){
						if(m_redis.sismember(DataList.dim_author_book_set + ":" + authorId,bookid)){
							it.remove();
							break;
						}
					}
					//continue;
				}
			}
		}
		
		//计算待推荐图书权重
//		m_logger.debug("PrefCompute book_list is (filter): "+m_bookId.toString());//test
		
		Iterator<String> it1 = m_bookId.iterator();
		while(it1.hasNext()){
			String bookid = it1.next();
			double sum = 0.0;
			Map<String, String> weight_map = new HashMap<String, String>();
			weight_map = m_redis.hgetAll(DataList.dm_bookid_tag_weight_hash + ":" + bookid);//取出该书的关联标签打分
			for(int i =0; i != m_tagsKeys.length; i++){
				if(weight_map.containsKey(m_tagsKeys[i])){
					double b =1.0;
					if(m_prefcom_flag == 0){
						b = Double.valueOf(weight_map.get(m_tagsKeys[i])).doubleValue();
					}
					
//					m_logger.debug("b val is : " + b);//test
//					m_logger.debug("m_tagsVals is : " + m_tagsVals[i]);//test
					
					sum += m_tagsVals[i]*b;
				}
			}		
			
			m_book_weight.add(new BookPrefData(bookid,sum));
			
//			m_logger.debug("m_book_weight size is : " + m_book_weight.size());//test
			
			if(m_book_weight.size() >= m_number+1){
				m_book_weight.remove(m_book_weight.first());
			}
		}
		
	}
	
	public ArrayList<String> getbookresult(){
		
		if(m_book_weight == null){
			return null;
		}
		
		Iterator<BookPrefData> it = m_book_weight.iterator();
		
		m_bookid = new ArrayList<String>();
		
		while (it.hasNext()) {  
		  m_bookid.add(it.next().getbookId());
		  
		}
		
		m_book_weight.clear();
		
		return m_bookid;
	}
}
