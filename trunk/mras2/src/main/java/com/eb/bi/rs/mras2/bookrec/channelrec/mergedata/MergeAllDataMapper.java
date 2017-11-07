package com.eb.bi.rs.mras2.bookrec.channelrec.mergedata;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MergeAllDataMapper extends Mapper<LongWritable, Text, Text, Text>{
	public static String datatype = "all";
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		String[] fields = value.toString().split("\\|",-1);//msisdn| sale_parameter|record_day
		if (fields.length == 3 && (!StringUtils.isBlank(fields[0])) && (!StringUtils.isBlank(fields[2]))) {	
			try {
				String msisdn = fields[0];
				String sale_parameter = fields[1];
				SimpleDateFormat sdf =  new SimpleDateFormat( "yyyyMMdd" );
				Long visitTime = sdf.parse(fields[2]).getTime();
				Calendar calendar = Calendar.getInstance();
				int month = calendar.get(Calendar.MONTH)-6;
				int year = calendar.get(Calendar.YEAR);
				if (month < 0) {
					month = month + 12;
					year--;
				}
				calendar.set(Calendar.YEAR, year);
				calendar.set(Calendar.MONTH, month);
				calendar.set(Calendar.HOUR_OF_DAY, 0); // 控制时
				calendar.set(Calendar.MINUTE, 0); // 控制分
				calendar.set(Calendar.SECOND, 0); // 控制秒
				System.out.println(calendar.getTime().getTime() - visitTime);
				if (calendar.getTime().getTime() - visitTime < 0) {
					String result = datatype + ";" + value.toString();
					context.write(new Text(msisdn + "|" + sale_parameter) , new Text(result));
				}
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
	}
	
}
