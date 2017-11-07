package com.eb.bi.rs.unifyrecs2hbase;

import org.apache.hadoop.hbase.util.Bytes;

public class Result2HbaseTable {

	public static final String CF = "cf";
	public static final byte[] BCF = Bytes.toBytes(CF);
	public static final byte[] BCOL_HOT = Bytes.toBytes("14");
	public static final byte[] BCOL_NEW = Bytes.toBytes("15");
	public static final byte[] BCOL_TAG = Bytes.toBytes("16");
	public static final byte[] BCOL_GUESS = Bytes.toBytes("17");
	public static final byte[] BCOL_FAMOUS = Bytes.toBytes("5");
	public static final byte[] BCOL_REC_TAG = Bytes.toBytes("tag");
	public static final byte[] BCOL_REC_PAGE = Bytes.toBytes("page");
}
