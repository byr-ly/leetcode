package com.eb.bi.rs.mras.bookrec.itemcf.similarity;

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 保存单个用户对两本图书之间相似度的贡献信息
 */
public class SimilarityWritable implements Writable {

	// 两本图书权重与评分的乘积
	private double wr;
	// 图书I的评分的平方
	private double ri;
	// 图书J的评分的平方
	private double rj;

	public SimilarityWritable() {
		
	}

	public SimilarityWritable(double wr, double ri, double rj) {
		this.wr = wr;
		this.ri = ri;
		this.rj = rj;
	}

	public void write(DataOutput out) throws IOException {
		out.writeDouble(wr);
		out.writeDouble(ri);
		out.writeDouble(rj);
	}

	public void readFields(DataInput in) throws IOException {
		wr = in.readDouble();
		ri = in.readDouble();
		rj = in.readDouble();
	}

	public double getWr() {
		return wr;
	}

	public double getRi() {
		return ri;
	}

	public double getRj() {
		return rj;
	}

	@Override
	public String toString() {
		return String.format("%f|%f|%f", wr, ri, rj);
	}
}
