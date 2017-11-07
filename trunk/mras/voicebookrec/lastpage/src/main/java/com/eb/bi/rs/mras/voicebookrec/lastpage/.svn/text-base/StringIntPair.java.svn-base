package com.eb.bi.rs.mras.voicebookrec.lastpage;

import java.io.*;
import org.apache.hadoop.io.*;



public class StringIntPair implements WritableComparable<StringIntPair> {
	private String first;
	private int second;
	
	public StringIntPair(){
		
	}	
	
	public StringIntPair(StringIntPair rhs){
		first = rhs.first;
		second = rhs.second;
	}
	
	public StringIntPair(String first, int second) {
		set(first, second);
	}
	  
    public void set(String first, int second) {    	
    	this.first = first;
    	this.second = second;
    }
  
    public String getFirst() {
    	return first;
    }

    public int getSecond() {    	
    	return second;
    }

    @Override
    public void write(DataOutput out) throws IOException {
    	out.writeUTF(first);
    	out.writeInt(second);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
    	this.first = in.readUTF();
    	this.second = in.readInt();
    }
  
    @Override
    public int hashCode() {    	
    	return first.hashCode() + second;
    }
  
    @Override
    public boolean equals(Object o) {
    	if (o instanceof StringIntPair) {    	
    		StringIntPair rhs = (StringIntPair) o;
    		return first.equals(rhs.first) && second == rhs.second;
    	}
    	return false;
    }  
  
    @Override
    public String toString() {
    	return first + "|" + second;
    }  
  
	@Override/*按second降序排列*/
	public int compareTo(StringIntPair o) {	
		if(o.second > this.second){
			return 1;
		}else if(o.second < this.second){
			return -1;
		}else{
			return this.first.compareTo(o.first);
		}
	}
}
	

  
 