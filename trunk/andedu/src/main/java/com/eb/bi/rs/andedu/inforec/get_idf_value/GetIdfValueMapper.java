package com.eb.bi.rs.andedu.inforec.get_idf_value;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by liyang on 2016/5/31.
 */
public class GetIdfValueMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
//	private String filterChar;
	private String segMethod;
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] fields = value.toString().split("\u0001", -1);
        if (fields.length < 4) {
            System.out.println("LoadNewsMapper: Bad Line: " + value.toString());
            return;
        }
        if ("测试".equals(fields[3])) {
        	fields[3] = "";
		}
        
        String infosTitle = FilterChar.clearNotChinese(fields[2]);
        String infosContent = FilterChar.clearNotChinese(fields[3]);
//        String infosTitle = fields[2].replaceAll(filterChar, " ");
//        infosTitle = infosTitle.replaceAll("[\\pP<>]", " ");
//        String infosContent = fields[3].replaceAll(filterChar, " ");
//        infosContent = infosContent.replaceAll("[\\pP<>]", " ");

        ArrayList<String> list = SegWord.segWord(segMethod, infosTitle, infosContent);
        for(int i = 0; i < list.size(); i++){
            context.write(new Text(list.get(i)),new IntWritable(1));
        }
    }
    
    @Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
//		filterChar = conf.get("filterChar", "[\\pP��$^=+~|│④Ⅰ1<>\r\n]");
		segMethod = conf.get("segMethod", "N");
	}
}
