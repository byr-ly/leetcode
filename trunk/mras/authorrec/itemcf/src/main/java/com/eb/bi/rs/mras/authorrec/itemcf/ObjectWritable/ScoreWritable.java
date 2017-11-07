package com.eb.bi.rs.mras.authorrec.itemcf.ObjectWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class ScoreWritable implements WritableComparable<ScoreWritable> {

    private String authorid;
    private double score;

    public ScoreWritable() {

    }

    public ScoreWritable(String id, double sc) {
        this.authorid = id;
        this.score = sc;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // TODO Auto-generated method stub
        out.writeUTF(this.authorid);
        out.writeDouble(this.score);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // TODO Auto-generated method stub
        this.authorid = in.readUTF();
        this.score = in.readDouble();
    }

    @Override
    public int compareTo(ScoreWritable o) {
        // TODO Auto-generated method stub
        return this.authorid.compareTo(o.authorid);
    }

    public String getAuthorId() {
        return this.authorid;
    }

    public double getScore() {
        return this.score;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ScoreWritable)) {
            return false;
        }
        ScoreWritable sw = (ScoreWritable) o;
        return authorid == sw.authorid || authorid.equals(sw.authorid);
    }

    @Override
    public int hashCode() {
        return authorid.hashCode();
    }

    @Override
    public String toString() {
        String str = String.format("%s|%f", authorid, score);
        return str;
    }


    public ScoreWritable clone() {
        ScoreWritable writable = new ScoreWritable(authorid, score);
        return writable;
    }

}
