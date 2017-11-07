package com.eb.bi.rs.mras.bookrec.qiangfarec.selectbooks;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class SelectHighlyRecommendBooksMapper extends Mapper<Object, Text, Text, Text>  {
	
	@Override
	protected void map(Object key, Text value,Context context) throws IOException ,InterruptedException {
		String[] fields = value.toString().split("\\|",-1);//msisdn用户|用户群|偏执程度|前三分类 |相似分类 |历史阅读分类
		if (fields.length >= 3 && !StringUtils.isBlank(fields[0])) {	
			String groupId = fields[1];
			String pref_level = fields[2];
			//如果用户为深度、中度或强偏执用户
			if ("2".equals(groupId) || "3".equals(groupId) || "3".equals(pref_level)) {	
				String result = "";
				if(fields.length >= 4){
					for(int i = 3 ; i < fields.length ; i++){
						result += fields[i] + "|";
					}					
				}
				context.write(new Text(fields[0]), new Text(result));
			}					
		}		
	}
}
