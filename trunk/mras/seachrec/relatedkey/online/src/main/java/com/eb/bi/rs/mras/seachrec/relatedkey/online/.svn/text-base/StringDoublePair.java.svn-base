package com.eb.bi.rs.mras.seachrec.relatedkey.online;




public class StringDoublePair implements Comparable<StringDoublePair> {
	private String first;
	private double second;
	
	public StringDoublePair(){
		
	}	
	
	public StringDoublePair(StringDoublePair rhs){
		set(rhs.first, rhs.second);
	}
	
	public StringDoublePair(String first, double second) {
		set(first, second);
	}
	  
    public void set(String first, double second) {    	
    	this.first = first;
    	this.second = second;
    }
  
    public String getFirst() {
    	return first;
    }

    public double getSecond() {    	
    	return second;
    }

  
    @Override
    public int hashCode() {
    	long temp = Double.doubleToLongBits(second);    	
    	return first.hashCode() + (int) (temp ^ (temp >>> 32));
    }
  
    @Override
    public boolean equals(Object o) {
    	if (o instanceof StringDoublePair) {    	
    		StringDoublePair rhs = (StringDoublePair) o;
    		return first.equals(rhs.first) && second == rhs.second;
    	}
    	return false;
    }  
  
    @Override
    public String toString() {
    	return first + "|" + second;
    }  
  
	@Override
	public int compareTo(StringDoublePair o) {//降序排列
		if(o.second > this.second){
			return 1;
		}else if(o.second < this.second){
			return -1;
		}else{
			return this.first.compareTo(o.first);
		}
	}
}
	

  
 