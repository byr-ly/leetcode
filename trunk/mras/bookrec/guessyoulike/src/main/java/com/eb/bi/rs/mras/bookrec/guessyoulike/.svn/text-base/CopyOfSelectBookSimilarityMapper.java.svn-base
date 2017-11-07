package com.eb.bi.rs.mras.bookrec.guessyoulike;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.eb.bi.rs.mras.bookrec.guessyoulike.util.StringDoublePair;



public class CopyOfSelectBookSimilarityMapper extends Mapper<Object, Text, Text, StringDoublePair> {
	//1213096|10722911|3302.0|1067.0|19.0|0.0057540885|0.017806936|0.0117805125|0.3231375|2
	//图书A|图书B|图书A用户数|图书B用户数|图书AB共同用户数|前置信度|后置信|KULC|IR|classtype
	//图书A|图书B|来源|相似度
	@Override
	protected void map(Object key, Text value,Context context) throws IOException ,InterruptedException {
		String[] fields = value.toString().split("\\|");
		if (fields.length == 10) {
			if ("1".equals(fields[9])) {
				context.write(new Text(fields[0]), new StringDoublePair(fields[1], Double.parseDouble(fields[7])));
			}
			
		}
		
	}

}
