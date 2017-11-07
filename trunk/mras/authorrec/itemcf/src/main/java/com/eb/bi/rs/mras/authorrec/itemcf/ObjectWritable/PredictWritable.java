package com.eb.bi.rs.mras.authorrec.itemcf.ObjectWritable;

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 计算预测得分中
 */
public class PredictWritable implements Writable {

    // 两个作者的相似性
    private float sim;
    // 用户对打分作者的打分-该作者的平均打分
    private float con;

    public PredictWritable() {

    }

    public PredictWritable(float si, float co) {
        this.sim = si;
        this.con = co;
    }

    public void write(DataOutput out) throws IOException {
        out.writeFloat(sim);
        out.writeFloat(con);
    }

    public void readFields(DataInput in) throws IOException {
        sim = in.readFloat();
        con = in.readFloat();
    }

    public float getSim() {
        return sim;
    }

    public float getCon() {
        return con;
    }

    @Override
    public String toString() {
        return String.format("%f|%f", sim, con);
    }

}
