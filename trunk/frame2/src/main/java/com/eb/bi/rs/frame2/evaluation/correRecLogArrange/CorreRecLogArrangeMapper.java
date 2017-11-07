package com.eb.bi.rs.frame2.evaluation.correRecLogArrange;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by chenyuxiao on 2016/08/28.
 */
public class CorreRecLogArrangeMapper extends Mapper<Object, Text, Text, NullWritable> {
	private String recType;
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        recType = conf.get("rec.typer", "3");
    }

    /**
    输入数据:
        关联推荐web日志（MyTimeLog）

    输出数据:
        key: msisdn用户 | 推荐位  |  看到的图书ID1, 看到的图书ID2, 看到的图书ID3... | 时间
        value: null
    */
    public void map(Object text, Text value, Context context) throws IOException, InterruptedException {
    	
        if (value.toString().indexOf("prtype=9") != -1) {
        	String[] fields = value.toString().split("\t");
        	String[] fields2 = value.toString().split("&");
            if (fields.length == 2 && fields2.length >= 3 && fields[0] != null && fields[1] != null && fields2[2].indexOf("msisdn") != -1 ) {// 数据不为空
            	String[] split = fields2[2].split("=");
            	if(split.length == 2){
            		String replace = fields[1].replace("|", ",");
            		String substring = replace.substring(0, replace.length()-1);
            		String[] split2 = substring.split(",");
            		if(split2.length >= 10){
            			context.write(new Text(split[1] + "|" + recType + "|" + substring + "|" +getNowdate()),NullWritable.get());
            		}
            	}
            }
        }
    }
    private String getNowdate() {//当前日期
        // 获取当前日期
        Date now = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd HH:mm:ss");//可以方便地修改日期格式
        String nowdate = dateFormat.format(now).substring(0, 8);
        return nowdate;
    }
}
