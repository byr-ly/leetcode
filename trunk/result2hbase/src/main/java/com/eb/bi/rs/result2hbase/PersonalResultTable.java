package com.eb.bi.rs.result2hbase;

import org.apache.hadoop.hbase.util.Bytes;

public class PersonalResultTable {

	public static final String CF = "cf";
	public static final byte[] BCF = Bytes.toBytes(CF);
	public static final byte[] BCOL = Bytes.toBytes("result");
}
