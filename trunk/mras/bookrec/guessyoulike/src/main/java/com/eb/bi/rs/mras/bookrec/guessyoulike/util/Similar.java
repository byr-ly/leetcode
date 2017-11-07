package com.eb.bi.rs.mras.bookrec.guessyoulike.util;

import java.util.ArrayList;
import java.util.List;

public class Similar {
	private List<Float> m_Sim = new ArrayList<Float>();
	
	//向量初始化话
	public void init(String similarString, String separator){
		String[] fields = similarString.toString().split(separator);
		
		for(int i = 0; i != fields.length; i++){
			m_Sim.add(Float.valueOf(fields[i]));
		}
	}
	
	//get向量
	public List<Float> getSim(){
		return m_Sim;
	}
	
	//向量和
	public void similarAdd(Similar Sim){
		for(int i = 0; i != m_Sim.size(); i++){
			m_Sim.set(i, m_Sim.get(i) + Sim.getSim().get(i));
		}
	}
	
	//向量与数乘
	public void similarMult(float bookScore){
		for(int i = 0; i != m_Sim.size(); i++){
			m_Sim.set(i, m_Sim.get(i) * bookScore);
		}
	}

	public void addString(String similarString, String separator) {
		if(m_Sim.size()==0){
			init(similarString, separator);
			return;
		}
		
		String[] fields = similarString.toString().split(separator);
		
		for(int i = 0; i != fields.length; i++){
			m_Sim.set(i, m_Sim.get(i) + Float.valueOf(fields[i]));
		}
	}

	public String tosimilarString(String separator) {
		String similarString = "";
		
		for(int i = 0; i != m_Sim.size(); i++){
			//similarString = m_Sim.get(i)+""+separator+similarString;
			similarString = similarString + m_Sim.get(i)+separator;
		}
		
		return similarString.substring(0, similarString.length()-separator.length());
	}

	public void clean() {
		m_Sim.clear();
	}
}
