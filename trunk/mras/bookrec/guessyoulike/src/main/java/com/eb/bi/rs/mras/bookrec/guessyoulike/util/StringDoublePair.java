package com.eb.bi.rs.mras.bookrec.guessyoulike.util;

import java.io.*;
import org.apache.hadoop.io.*;



public class StringDoublePair implements WritableComparable<StringDoublePair> {
	private String first;
	private double second;
	private String fieldDelimiter = "|";
	
	public StringDoublePair(){
		
	}	
	
	public StringDoublePair(StringDoublePair rhs){
		set(rhs.first, rhs.second, rhs.fieldDelimiter);
	}
	
	
	public StringDoublePair(String first, double second) {
		set(first, second );
	}
	
	public StringDoublePair(String first, double second, String fieldDelimiter) {
		set(first, second, fieldDelimiter);
	}
	
	public void set(String first, double second) { 
		this.first = first;
		this.second = second;	    	
	 }
	  
    public void set(String first, double second, String fieldDelimiter) {    	
    	this.first = first;
    	this.second = second;
    	this.fieldDelimiter = fieldDelimiter;
    }
  
    public String getFirst() {
    	return first;
    }

    public double getSecond() {    	
    	return second;
    }
    
    public String getFieldDelimiter() {
    	return fieldDelimiter;
    }

    @Override
    public void write(DataOutput out) throws IOException {
    	out.writeUTF(first);
    	out.writeUTF(fieldDelimiter);
    	out.writeDouble(second);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
    	this.first = in.readUTF();
    	this.fieldDelimiter = in.readUTF();
    	this.second = in.readDouble();
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
    	return first + fieldDelimiter + second;
    }  
  
	@Override
	public int compareTo(StringDoublePair o) {	
		if(this.second > o.second){
			return 1;
		}else if(this.second < o.second){
			return -1;
		}else{
			return this.first.compareTo(o.first);
		}
	}
}
	

  
 