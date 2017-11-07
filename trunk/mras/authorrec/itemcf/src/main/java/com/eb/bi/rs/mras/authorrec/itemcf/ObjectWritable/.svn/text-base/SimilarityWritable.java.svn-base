package com.eb.bi.rs.mras.authorrec.itemcf.ObjectWritable;

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 保存单个用户对两个作者之间相似度的打分信息
 */
public class SimilarityWritable implements Writable {

    // 两个作者评分的乘积
    private float wr;
    // 作者I的评分的平方
    private float ri;
    // 作者J的评分的平方
    private float rj;

    public SimilarityWritable() {

    }

    public SimilarityWritable(float wr, float ri, float rj) {
        this.wr = wr;
        this.ri = ri;
        this.rj = rj;
    }

    public void write(DataOutput out) throws IOException {
        out.writeFloat(wr);
        out.writeFloat(ri);
        out.writeFloat(rj);
    }

    public void readFields(DataInput in) throws IOException {
        wr = in.readFloat();
        ri = in.readFloat();
        rj = in.readFloat();
    }

    public float getWr() {
        return wr;
    }

    public float getRi() {
        return ri;
    }

    public float getRj() {
        return rj;
    }

    @Override
    public String toString() {
        return String.format("%.3f|.3%f|.3%f", wr, ri, rj);
    }
}
