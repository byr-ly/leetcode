package com.eb.bi.rs.frame2.algorithm.occurrence;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

public class Items {
	
	private Map<String,Float> m_bookordernumMap = new TreeMap<String, Float>();
	
	//private List<String> m_pairString = new ArrayList<String>();
	
	private String m_Allid = new String();
	private String m_Allidnum = new String();
	private String m_TopNid = new String();
	private String m_TopNidnum = new String();
	
	private Integer m_topN;
	//private String m_outWhich;
	
	private String m_item_keyId;
	
	private String m_kv_separator;
	private String m_pair_separator;
	
	//private Float m_keyIdnum = new Float(1.0);
	
	
	
	public void setkeyId(String itemId){
		m_item_keyId = itemId;
	}
	
	public void setconf(Integer topN, String kv_separator, String pair_separator){
		m_topN = topN;
		m_kv_separator = kv_separator;
		m_pair_separator = pair_separator;
	}

	public void addAll(Map<String,Float> bookordernumMap){
		Iterator<Entry<String, Float>> iterator = bookordernumMap.entrySet().iterator();
		 while(iterator.hasNext()){
			 Entry<String, Float> entry = iterator.next();
			 if(m_bookordernumMap.containsKey(entry.getKey())){
				 m_bookordernumMap.put(entry.getKey(), m_bookordernumMap.get(entry.getKey()) + entry.getValue());
			 }
			 else {
				 m_bookordernumMap.put(entry.getKey(), entry.getValue());
			}
		 }
		
	}
	/*
	public void change2listString(){
		Iterator<Entry<String, Long>> iterator = m_bookordernumMap.entrySet().iterator();
		 while(iterator.hasNext()){
			 Entry<String, Long> entry = iterator.next();
			 m_pairString.add(entry.getKey()+m_kv_separator+entry.getValue().toString());
		 }
	}
	
	public String getpairString(int index){
		return m_pairString.get(index);
	}
	*/
	public void add(String k,Float v){
		//String item = k;
		//Float num = v;
		
		if(m_bookordernumMap.containsKey(k)){
			m_bookordernumMap.put(k, m_bookordernumMap.get(k) + v);
		}
		else {
			m_bookordernumMap.put(k, v);
		}
	}
	
	public Map<String,Float> getitemsMap(){
		return m_bookordernumMap;
	}
	
	public String getString(int outWhich){

		switch (outWhich) {
		case 0:
			return m_Allid;
		case 1:
			return m_Allidnum;
		case 2:
			return m_TopNid;
		case 3:
			return m_TopNidnum;
		default:
			return m_Allidnum;
		}
	}
	
	public void generateString(){
		m_Allid = "";
		m_Allidnum = "";
		m_TopNid = "";
		m_TopNidnum = "";
		
		if(m_bookordernumMap.size()==0){
			return;
		}
		
		String tmpString1 = new String();
		String tmpString2 = new String();
			
		int topnum = m_topN.intValue() < m_bookordernumMap.size() ? m_topN.intValue():m_bookordernumMap.size();
			
		List<Map.Entry<String, Float>> infoIds = new ArrayList<Map.Entry<String, Float>>( 
					m_bookordernumMap.entrySet()); 
			
		
		//排序
		Collections.sort(infoIds, new Comparator<Map.Entry<String, Float>>(){
			public int compare(Map.Entry<String, Float> o1,
			Map.Entry<String, Float> o2){
			return ((int)(o2.getValue() - o1.getValue()));
			}
			});
		
		//System.out.println(topnum);
		
		for(int i = 0; i != infoIds.size(); i++){
			/*
			if(m_item_keyId.equals(infoIds.get(i).getKey())){
				//m_keyIdnum = infoIds.get(i).getValue();
				topnum = topnum + 1;
				continue;
			}
			*/
			
			tmpString1 = infoIds.get(i).getKey() + m_kv_separator 
					+ infoIds.get(i).getValue().toString();
			tmpString2 = infoIds.get(i).getKey();
			
			if(i<topnum){
				m_TopNidnum += tmpString1 + m_pair_separator;
				m_TopNid += tmpString2 + m_pair_separator;
			}
			
			m_Allidnum += tmpString1 + m_pair_separator;
			m_Allid += tmpString2 + m_pair_separator;
		}
		
		//System.out.println(m_pair_separator.length());
		
		m_TopNidnum = m_TopNidnum.substring(0,m_TopNidnum.length() - m_pair_separator.length());
		m_Allidnum = m_Allidnum.substring(0,m_Allidnum.length() - m_pair_separator.length());
		m_TopNid = m_TopNid.substring(0,m_TopNid.length() - m_pair_separator.length());
		m_Allid = m_Allid.substring(0,m_Allid.length() - m_pair_separator.length());
		
		tmpString1 = "";
		tmpString2 = "";	
	}

	public void clean() {
		// TODO Auto-generated method stub
		m_bookordernumMap.clear();
		m_Allid = "";
		m_Allidnum = "";
		m_TopNid = "";
		m_TopNidnum = "";
		//m_pairString.clear();
	}
}
