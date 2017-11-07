package com.eb.bi.rs.mras.bookrec.qiangfarec.sortofzone;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BookInfoQiangfaMapper extends Mapper<Object, Text, Text, Text> {

	private String fieldDelimiter;
	private double recPoints;
	private double newBookPoints;
	private long newBookTime;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		fieldDelimiter = conf.get("field.delimiter", "\\|");
		recPoints = Double.parseDouble(conf.get("rec.book.edit.point", "0.2"));
		newBookPoints = Double.parseDouble(conf.get("new.book.edit.point", "0,1"));
		newBookTime = Long.parseLong(conf.get("new.book.time", "30")) * 24 * 3600 * 1000;//单位毫秒
	}

	// 专区id | bookid |事业部| if_rec是否强推| if_on是否在当前专区中| ontime 上专区时间
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] fields = value.toString().split(fieldDelimiter);
		
		if (fields[4].equals("1")) {// 图书if_on在当前专区中
			double editPoint = 0.0;
	
			if (fields[3].equals("1")) { // 强推图书
				editPoint += recPoints;
			}
			else{ // 不是强推图书，但是有可能是新上架图书
				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
				try {
					Date ontime = sdf.parse(fields[5]);
				    Date nowDate = sdf.parse(sdf.format(new Date()));
				    long diff = nowDate.getTime() - ontime.getTime();//单位毫秒
				    
				    if (diff <= newBookTime) { // 新上架图书
						editPoint += newBookPoints; 
					}
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			DecimalFormat df = new DecimalFormat("#0.0000"); //保留4位小数 
			String dataValue = fields[0] + "|" + fields[1] + "|" + df.format(editPoint); // 专区id|bookid|编辑分
			
			context.write(new Text(fields[1]), new Text(dataValue));
		}
	}
}
