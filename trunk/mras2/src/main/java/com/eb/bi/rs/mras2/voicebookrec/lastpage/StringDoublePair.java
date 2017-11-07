package com.eb.bi.rs.mras2.voicebookrec.lastpage;

import java.io.*;
import org.apache.hadoop.io.*;



public class StringDoublePair implements WritableComparable<StringDoublePair> {
	private String first;
	private double second;
	
	public StringDoublePair(){
		
	}	
	
	public StringDoublePair(StringDoublePair rhs){
		first = rhs.first;
		second = rhs.second;
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
    public void write(DataOutput out) throws IOException {
    	out.writeUTF(first);
    	out.writeDouble(second);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
    	this.first = in.readUTF();
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
    	return first + "|" + second;
    }  
  
	@Override
	public int compareTo(StringDoublePair o) {	
		if(o.second > this.second){
			return 1;
		}else if(o.second < this.second){
			return -1;
		}else{
			return this.first.compareTo(o.first);
		}
	}
}
	

  
 