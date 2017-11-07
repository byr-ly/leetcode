package com.eb.bi.rs.frame.recframe.resultcal.offline.selector.mr;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RandomTopSelectorMapper extends Mapper<Object, Text, Text, Text> {
	private String fieldDelimiter;
	private String keyFieldIdxs;
	
	
	@Override
	protected void map(Object key, Text value, Context context) throws java.io.IOException ,InterruptedException {
		String[] fields = null;
		if(fieldDelimiter.equals("|")){
			fields = value.toString().split("\\|");
		}else {
			fields = value.toString().split(fieldDelimiter);
		}
		String[] keyFieldIdxArr = keyFieldIdxs.split(",");
		String keyField = "";
		for(int i = 0; i < keyFieldIdxArr.length; ++i){			
			keyField += fields[Integer.parseInt(keyFieldIdxArr[i])] + fieldDelimiter;	
		}
		context.write(new Text(keyField), value);	
	}
	
	@Override
	protected void setup(Context context) throws java.io.IOException ,InterruptedException {		
		
		Configuration conf = context.getConfiguration();
		fieldDelimiter = conf.get("field.delimiter","|");
		keyFieldIdxs = conf.get("key.field.indexes","0");/*以逗号分隔*/
	}

}
