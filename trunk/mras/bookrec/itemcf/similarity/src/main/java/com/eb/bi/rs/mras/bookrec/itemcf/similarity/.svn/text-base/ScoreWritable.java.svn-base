package com.eb.bi.rs.mras.bookrec.itemcf.similarity;

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 保存单个用户对一本书的评分信息
 */
public class ScoreWritable implements WritableComparable<ScoreWritable> {

	// 图书id
	private String id;
	// 用户评分
	private double score;

	public ScoreWritable() {

	}

	public ScoreWritable(String id, double score) {
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

	public int compareTo(ScoreWritable o) {
		return id.compareTo(o.id);
	}

	public String getId() {
		return id;
	}

	public double getScore() {
		return score;
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof ScoreWritable)) {
			return false;
		}
		ScoreWritable sw = (ScoreWritable) o;
		return id == sw.id || id.equals(sw.id);
	}

	@Override
	public int hashCode() {
		return id.hashCode();
	}

	@Override
	public String toString() {
		return id + "|" + score;
	}
}
