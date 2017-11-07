package com.eb.bi.rs.mras.seachrec.keyseach.offline;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class TagDictWritable implements WritableComparable<TagDictWritable> {
	private String tag;
	private int weight;
	public TagDictWritable() {
		
	}
	public TagDictWritable(String tag, int weight) {
		super();
		this.tag = tag;
		this.weight = weight;
	}
	public String getTag() {
		return tag;
	}
	public void setTag(String tag) {
		this.tag = tag;
	}
	public int getWeight() {
		return weight;
	}
	public void setWeight(int weight) {
		this.weight = weight;
	}
	

	@Override	
	public void write(DataOutput out) throws IOException {
		out.writeUTF(tag);
		out.writeInt(weight);
		
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.tag = in.readUTF();
		this.weight = in.readInt();
	}
	
	
	@Override
	public int compareTo(TagDictWritable o) {		

		if(this.weight > o.weight){
			return 1;
		}else if(this.weight < o.weight){
			return -1;
		}else{
			return this.tag.compareTo(o.tag);
		}
	}
	
	
	@Override
	public String toString() {
		return tag + "|" + weight;
	}	

}
