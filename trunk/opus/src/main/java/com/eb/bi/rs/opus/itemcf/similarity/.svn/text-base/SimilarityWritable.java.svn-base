package com.eb.bi.rs.opus.itemcf.similarity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/*
 * ynn 2016-07-21
 * 保存单个用户对两个动漫之间相似度的贡献信息
 */
public class SimilarityWritable implements Writable {

	// 两个动漫评分的乘积
	private double productij;
	// 动漫I的评分的平方
	private double squarei;
	// 动漫J的评分的平方
	private double squarej;

	public SimilarityWritable() {

	}

	public SimilarityWritable(double productij, double squarei, double squarej) {
		this.productij = productij;
		this.squarei = squarei;
		this.squarej = squarej;
	}

	public void readFields(DataInput in) throws IOException {
		productij = in.readDouble();
		squarei = in.readDouble();
		squarej = in.readDouble();
	}

	public void write(DataOutput out) throws IOException {
		out.writeDouble(productij);
		out.writeDouble(squarei);
		out.writeDouble(squarej);
	}

	public double getProductij() {
		return productij;
	}

	public double getSquarei() {
		return squarei;
	}

	public double getSquarej() {
		return squarej;
	}

}
