package com.eb.bi.rs.frame2.algorithm.itemcf.similarity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CosineSimilarityWritable implements Writable{
	
	// 两个物品的乘积
	private double productij;
	// 物品I的评分的平方
	private double squarei;
	// 物品J的评分 的平方
	private double squarej;
	
	public CosineSimilarityWritable() {
		
	}

	public CosineSimilarityWritable(double productij, double squarei, double squarej){
		this.productij = productij;
		this.squarei = squarei;
		this.squarej = squarej;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(productij);
		out.writeDouble(squarei);
		out.writeDouble(squarej);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		productij = in.readDouble();
		squarei = in.readDouble();
		squarej = in.readDouble();
	}

	public double getProductij() {
		return productij;
	}

	public void setProductij(double productij) {
		this.productij = productij;
	}

	public double getSquarei() {
		return squarei;
	}

	public void setSquarei(double squarei) {
		this.squarei = squarei;
	}

	public double getSquarej() {
		return squarej;
	}

	public void setSquarej(double squarej) {
		this.squarej = squarej;
	}
	
}
