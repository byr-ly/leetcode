package com.eb.bi.rs.frame.recframe.resultcal.offline.sorter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CompositeKey implements WritableComparable<CompositeKey> {
	

	private String key;
	private String value;
	
	public CompositeKey() {
	}
	
	public CompositeKey(String key, String value) {	
		set(key, value);
	}	
	
	public void set(String key, String value){
		this.key = key;
		this.value = value;
		
	}


	@Override
	public int hashCode() {
		return key.hashCode() * 163 + value.hashCode();
	}
  
	@Override
	public boolean equals(Object o) {
		if (o instanceof CompositeKey) {	    	
			CompositeKey tp = (CompositeKey) o;
			return key.equals(tp.key) && value.equals(tp.value);
		}
		return false;
	}

	@Override
	public String toString() {
		return key + "\t" + value;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		key = in.readUTF();
		value = in.readUTF();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(key);
		out.writeUTF(value);
	}
	
	@Override
	public int compareTo(CompositeKey o) {	
		int result = key.compareTo(o.key);
		if (0 == result) {
			result = value.compareTo(o.value);
		}
		return result;
	}
	

	public String getKey() {	
		return key;
	}
	
	public void setKey(String key) {	
		this.key = key;
	}
	
	public String getValue() {	
		return value;
	}
	
	public void setValue(String value) {	
		this.value = value;
	}
}