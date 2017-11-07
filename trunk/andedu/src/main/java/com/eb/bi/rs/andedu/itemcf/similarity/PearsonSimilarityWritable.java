package com.eb.bi.rs.andedu.itemcf.similarity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PearsonSimilarityWritable implements Writable{
	
	private double i;
	private double j;
	
	public PearsonSimilarityWritable() {
		
	}

	public PearsonSimilarityWritable(double i, double j){
		this.i = i;
		this.j = j;
	}
	public void write(DataOutput out) throws IOException {
		out.writeDouble(i);
		out.writeDouble(j);
	}

	public void readFields(DataInput in) throws IOException {
		i = in.readDouble();
		j = in.readDouble();
	}

	public double getI() {
		return i;
	}

	public void setI(double i) {
		this.i = i;
	}

	public double getJ() {
		return j;
	}

	public void setJ(double j) {
		this.j = j;
	}
}
