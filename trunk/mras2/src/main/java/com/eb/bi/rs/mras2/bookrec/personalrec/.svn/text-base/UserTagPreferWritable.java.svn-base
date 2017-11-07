package com.eb.bi.rs.mras2.bookrec.personalrec;

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class UserTagPreferWritable implements WritableComparable<UserTagPreferWritable> {

	private String tag;
	private String id;
	private long order;
	private double preference;

	public UserTagPreferWritable() {
		
	}

	public UserTagPreferWritable(String tag, String id, long order, double preference) {
		this.tag = tag;
		this.id = id;
		this.order = order;
		this.preference = preference;
	}

	public UserTagPreferWritable(UserTagPreferWritable o) {
		tag = o.tag;
		id = o.id;
		order = o.order;
		preference = o.preference;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(tag);
		out.writeUTF(id);
		out.writeLong(order);
		out.writeDouble(preference);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		tag = in.readUTF();
		id = in.readUTF();
		order = in.readLong();
		preference = in.readDouble();
	}

	@Override
	public int compareTo(UserTagPreferWritable o) {
		if (preference != o.preference) {
			return preference < o.preference ? 1 : -1;
		} else if (order != o.order) {
			return (int) (o.order - order);
		} else {
			return id.compareTo(o.id);
		}
	}

	public String getTag() {
		return tag;
	}

	public String getId() {
		return id;
	}

	@Override
	public String toString() {
		return tag + "|" + id + "|" + order + "|" + preference;
	}
}
