package com.eb.bi.rs.andedu.inforec.filler_infos;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;

public class FillerInfosMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	private String fillerTime;
	
//	@Override
//	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//		System.out.println("begin map");
//		String[] fields = value.toString().split("\u0001", -1);
//	    if (fields.length < 9) {
//	    	System.out.println("FillerInfosMapper: Bad Line: " + value.toString());
//	    	return;
//	    }
//	    DateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
//	    Date dt1 = new Date();
//	    Date dt2 = null;
//	    
//	    Calendar now = Calendar.getInstance();  
//        now.setTime(dt1);  
//        now.set(Calendar.DATE, now.get(Calendar.DATE) - Integer.parseInt(fillerTime));  
//		
//        try {
//			dt1 = df.parse(df.format(now.getTime()));
//			dt2 = df.parse(fields[6]);
//		} catch (ParseException e) {
//			System.out.println("FillerInfosMapper: Bad Time: " + df.format(now.getTime()) + "|" + fields[6]);
//		}
//	    if (dt2.getTime() > dt1.getTime()) {
//	    	System.out.println("++++: "+fields[0]+fields[4]);
//	    	Log.info("++++: "+fields[0]+ " "+fields[4]);
//	    	context.write(new Text(fields[0].toString()), new Text(fields[4].toString()));
//	    }
//	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		System.out.println("begin map");
		String[] fields = value.toString().split("\t", -1);
	    if (fields.length < 9) {
	    	System.out.println("FillerInfosMapper: Bad Line: " + value.toString());
	    	return;
	    }
	    DateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
	    Date dt1 = new Date();
	    Date dt2 = null;
	    
	    Calendar now = Calendar.getInstance();  
        now.setTime(dt1);  
        now.set(Calendar.DATE, now.get(Calendar.DATE) - Integer.parseInt(fillerTime));  
		
        try {
			dt1 = df.parse(df.format(now.getTime()));
			dt2 = df.parse(fields[6]);
		} catch (ParseException e) {
			System.out.println("FillerInfosMapper: Bad Time: " + df.format(now.getTime()) + "|" + fields[6]);
		}
	    if (dt2.getTime() > dt1.getTime()) {
	    	context.write(new Text("all"), new Text(fields[0].toString() + "|" + fields[4].toString()));
	    }
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Logger log = Logger.getLogger(FillerInfosMapper.class);
		Configuration conf = context.getConfiguration();
		fillerTime = conf.get("fillerTime", "10");
	} 
}
