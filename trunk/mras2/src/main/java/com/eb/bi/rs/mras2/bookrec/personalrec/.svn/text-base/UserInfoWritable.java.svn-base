package com.eb.bi.rs.mras2.bookrec.personalrec;

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

class UserInfoWritable implements Writable {

	public static final int HISTORY = 0;
	public static final int CURRENT = 1;

	public int flag;
	public String info;

	public UserInfoWritable() {
		
	}

	public UserInfoWritable(int flag, String info) {
		this.flag = flag;
		this.info = info;
	}

	public UserInfoWritable(UserInfoWritable o) {
		flag = o.flag;
		info = o.info;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(flag);
		out.writeUTF(info);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		flag = in.readInt();
		info = in.readUTF();
	}

	@Override
	public String toString() {
		return flag + "|" + info;
	}
}
