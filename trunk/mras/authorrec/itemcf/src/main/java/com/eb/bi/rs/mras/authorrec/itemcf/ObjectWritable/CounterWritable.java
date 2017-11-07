package com.eb.bi.rs.mras.authorrec.itemcf.ObjectWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CounterWritable implements WritableComparable<CounterWritable> {

    private String oid;
    private int count;

    public CounterWritable() {

    }

    public CounterWritable(String id, int sc) {
        this.oid = id;
        this.count = sc;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // TODO Auto-generated method stub
        out.writeUTF(this.oid);
        out.writeInt(this.count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // TODO Auto-generated method stub
        this.oid = in.readUTF();
        this.count = in.readInt();
    }

    @Override
    public int compareTo(CounterWritable o) {
        // TODO Auto-generated method stub
        return this.oid.compareTo(o.oid);
    }

    public String getId() {
        return this.oid;
    }

    public int getCount() {
        return this.count;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof CounterWritable)) {
            return false;
        }
        CounterWritable sw = (CounterWritable) o;
        return oid == sw.oid || oid.equals(sw.oid);
    }

    @Override
    public int hashCode() {
        return oid.hashCode();
    }

    @Override
    public String toString() {
        String str = String.format("%s|%d", oid, count);
        return str;
    }


    public CounterWritable clone() {
        CounterWritable writable = new CounterWritable(oid, count);
        return writable;
    }

}
