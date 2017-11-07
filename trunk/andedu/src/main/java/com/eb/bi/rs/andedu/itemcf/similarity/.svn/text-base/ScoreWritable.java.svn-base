package com.eb.bi.rs.andedu.itemcf.similarity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 用户|物品|分
 */
public class ScoreWritable implements Writable {
	
	private String id;
	private double score;
	
	public ScoreWritable(){
		
	}
	
	public ScoreWritable(String id, double score){
		this.id = id;
		this.score = score;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(id);
		out.writeDouble(score);
	}

	public void readFields(DataInput in) throws IOException {
		id = in.readUTF();
		score = in.readDouble();
	}
	
	@Override
	public boolean equals(Object obj) {
		if(!(obj instanceof ScoreWritable)){
			return false;
		}
		ScoreWritable sw = (ScoreWritable) obj;
		return id == sw.id || id.equals(sw.id);
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}
	
}
