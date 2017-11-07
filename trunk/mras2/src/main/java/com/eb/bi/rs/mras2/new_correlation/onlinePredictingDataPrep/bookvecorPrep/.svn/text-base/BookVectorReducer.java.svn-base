package com.eb.bi.rs.mras2.new_correlation.onlinePredictingDataPrep.bookvecorPrep;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * created by LiuJie on 2017/08/03.
 */
public class BookVectorReducer extends Reducer<Text, Text, Text, NullWritable> {
	String currentBookEleganceAndVugarityScores =null;
	String currentBookBaseInfo = null;
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        /**
         * 输入数据分两种。Key均为 bookid。
         *
         * 1. 第一种值为A|图书雅俗分
         * 2. 第二种值为B|图书基础信息
         * */
    	//当前用户
    	String bookId = key.toString();
    	
        Iterator<Text> it = values.iterator();
        while (it.hasNext()) {

            String currentLine = it.next().toString();
            String[] fields = currentLine.split("\\|", -1);
            if (fields[0].equals("A")) {
                currentBookEleganceAndVugarityScores = currentLine.substring(fields[1].length()+fields[0].length()+2, currentLine.length());
            }else if (fields[0].equals("B")) {
                currentBookBaseInfo = currentLine.substring(fields[1].length()+fields[0].length()+2, currentLine.length());
            }
        }
       
        if (currentBookBaseInfo == null) {
            return;
        }
        if(currentBookEleganceAndVugarityScores==null){
        	currentBookEleganceAndVugarityScores = "0.0|";
        }
        /**
         * 拼接所有的图书特征
         */
		String sb = bookId +";"+ currentBookBaseInfo +"|"+ currentBookEleganceAndVugarityScores;
		context.write(new Text(sb), NullWritable.get());


    }
}