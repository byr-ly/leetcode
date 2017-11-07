package com.eb.bi.rs.mras.authorrec.itemcf.ObjectWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.eb.bi.rs.mras.authorrec.itemcf.recommend.RecommendItem;
import org.apache.hadoop.io.WritableComparable;


public class RecommItemWritable implements WritableComparable<RecommItemWritable> {

    private RecommendItem recommendItem = null;
    private int type;

    public RecommItemWritable() {

    }

    public RecommItemWritable(RecommendItem item, int t) {
        this.recommendItem = item;
        this.type = t;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // TODO Auto-generated method stub
        out.writeUTF(recommendItem.getAuthorId());
        out.writeUTF(recommendItem.getBookId());
        out.writeDouble(recommendItem.getScore());
        out.writeInt(this.type);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // TODO Auto-generated method stub
        String authorid = in.readUTF();
        String bookid = in.readUTF();
        double score = in.readDouble();
        this.recommendItem = new RecommendItem(authorid, bookid, score);
        this.type = in.readInt();
    }

    @Override
    public int compareTo(RecommItemWritable o) {
        // TODO Auto-generated method stub
        int comp = this.recommendItem.getAuthorId()
                .compareTo(o.recommendItem.getAuthorId());
        if (comp == 0) {
            comp = this.recommendItem.getBookId()
                    .compareTo(o.recommendItem.getBookId());
        }
        return comp;
    }

    public String getAuthorId() {
        return this.recommendItem.getAuthorId();
    }

    public String getBookId() {
        return this.recommendItem.getBookId();
    }

    public double getScore() {
        return this.recommendItem.getScore();
    }

    public int getType() {
        return this.type;
    }

    public RecommendItem getItem() {
        return this.recommendItem;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof RecommItemWritable)) {
            return false;
        }
        RecommItemWritable sw = (RecommItemWritable) o;
        boolean bAE1 = (this.recommendItem.getAuthorId()
                == sw.recommendItem.getAuthorId());
        boolean bAE2 = (this.recommendItem.getAuthorId()
                .equals(sw.recommendItem.getAuthorId()));
        boolean bBE1 = (this.recommendItem.getBookId()
                == sw.recommendItem.getBookId());
        boolean bBE2 = (this.recommendItem.getBookId()
                .equals(sw.recommendItem.getBookId()));
        return ((bAE1 || bAE2) && (bBE1 || bBE2));
    }

    @Override
    public int hashCode() {
//		String temp = String.format("%s|%s", this.recommendItem.getAuthorId(), 
//				this.recommendItem.getBookId());
//		return temp.hashCode();
        return this.recommendItem.getAuthorId().hashCode();
    }

    @Override
    public String toString() {
        String str = String.format("%s|%d", recommendItem.toString(), type);
        return str;
    }


    public RecommItemWritable clone() {
        RecommendItem newItem = this.recommendItem.clone();
        RecommItemWritable writable = new RecommItemWritable(newItem, type);
        return writable;
    }

}
