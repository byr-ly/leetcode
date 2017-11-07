package com.eb.bi.rs.frame.recframe.resultcal.offline.converter;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



/*横转竖*/
public class H2VConverterMapper extends Mapper<Object, Text , NullWritable, Text> {	
	
	private String SingleMultiFieldDelimiter;
	private String multiGroupDelimiter;
	private String multiFieldDelimiter;
	private String resultFieldDelimiter;

	private ArrayList<Integer> singleFieldIdxList = new ArrayList<Integer>();
	private ArrayList<Integer> reservedMultiFieldIdxList = new ArrayList<Integer>();
	private ArrayList<Integer> multiUnrelatedFieldIdxList;
	
	private int multiGroupContainsFieldNum;	
	private int multiGroupSetIdx;//跟上面的multiUnrelatedFieldIdxList配置一个即可	
	
	private boolean singleFirst;

	
	

	@Override
	protected void map(Object key, Text value, Context context) throws java.io.IOException ,InterruptedException {
		
		String[] fields;
		if(SingleMultiFieldDelimiter.equals("|")){
			fields = value.toString().split("\\|");			
		}else {
			fields = value.toString().split(SingleMultiFieldDelimiter);
		}
		
		ArrayList<Integer> adjustedMultiUnrelatedFieldIdxList = null;
		
		if(multiUnrelatedFieldIdxList != null){
			adjustedMultiUnrelatedFieldIdxList = new ArrayList<Integer>();
			for (int i = 0; i < multiUnrelatedFieldIdxList.size(); i++) {
				if(multiUnrelatedFieldIdxList.get(i) < 0){
					adjustedMultiUnrelatedFieldIdxList.add(i, fields.length + multiUnrelatedFieldIdxList.get(i));
				}else {
					adjustedMultiUnrelatedFieldIdxList.add(i, multiUnrelatedFieldIdxList.get(i));
					
				}				
			}			
		}
		
		ArrayList<Integer> adjustedSingleFieldIdxList = new ArrayList<Integer>();
		for(int i = 0; i < singleFieldIdxList.size(); i++){
			if(singleFieldIdxList.get(i) < 0) {
				adjustedSingleFieldIdxList.add(i, fields.length + singleFieldIdxList.get(i));
			}else {
				adjustedSingleFieldIdxList.add(i, singleFieldIdxList.get(i));
			}
		}		
		
		String singleField = "";
		for (int index : adjustedSingleFieldIdxList) {
			singleField += fields[index] + resultFieldDelimiter;				
		}
		singleField = singleField.substring(0, singleField.length() - 1);
		
		
		if (singleFirst) {
			
			if(multiGroupDelimiter.equals(SingleMultiFieldDelimiter)){
				if (multiGroupContainsFieldNum == 1) {
					for (int i = 0; i < fields.length; i++) {								
						if(!adjustedMultiUnrelatedFieldIdxList.contains(i)) {																				
							//context.write(new Text( singleField + resultFieldDelimiter + fields[i]), NullWritable.get());
							context.write(NullWritable.get(), new Text( singleField + resultFieldDelimiter + fields[i]));		
						}							
					}					
				}else {
					if (multiFieldDelimiter.equals(multiGroupDelimiter)) {
						int counter = 0;
						String result = singleField;
						for (int i = 0; i < fields.length; i++) {								
							if(!adjustedMultiUnrelatedFieldIdxList.contains(i)) {
								int index = (counter++) % multiGroupContainsFieldNum;
								if (reservedMultiFieldIdxList.contains(index)) {										
									result += resultFieldDelimiter + fields[i];	
									if (index == reservedMultiFieldIdxList.size() -1) {
										//context.write(new Text(result), NullWritable.get());
										context.write(NullWritable.get(), new Text(result));
										result = singleField;																					
									}									
								}
							}							
						}
					}else {
						for (int i = 0; i < fields.length; i++) {
							if(!adjustedMultiUnrelatedFieldIdxList.contains(i)) {													
								String[] fields1;
								if(multiFieldDelimiter.equals("|")){
									fields1 = fields[i].split("\\|");			
								}else {
									fields1 = fields[i].split(multiFieldDelimiter);
								}
								String result =  singleField;									
								for (int index : reservedMultiFieldIdxList) {
									result += resultFieldDelimiter + fields1[index];
								}
								//context.write(new Text(result), NullWritable.get());
								context.write(NullWritable.get(), new Text(result));
							}							
						}							
					}
				}
			}else {
				String[] multiGroups = null;				
				if(multiGroupDelimiter.equals("|")){
					multiGroups = fields[multiGroupSetIdx].split("\\|");			
				}else {
					multiGroups = fields[multiGroupSetIdx].split(multiGroupDelimiter);
				}
				
				if (multiGroupContainsFieldNum == 1) {//此时无从定义multiFieldDelimiter，其实此时multiFieldDelimiter应该为空
					for (int i = 0; i < multiGroups.length; i++) {																									
						//context.write(new Text(singleField + resultFieldDelimiter + multiGroups[i]), NullWritable.get());
						context.write(NullWritable.get(), new Text(singleField + resultFieldDelimiter + multiGroups[i]));
					}		
									
				}else {
					if (multiFieldDelimiter.equals(multiGroupDelimiter)) {//此时multiGroups就是multifield。
						int counter = 0;
						String result = singleField;
						for (int i = 0; i < multiGroups.length; i++) {					
							int index = (counter++) % multiGroupContainsFieldNum;
							if (reservedMultiFieldIdxList.contains(index)) {										
								result += resultFieldDelimiter + multiGroups[i];	
								if (index == reservedMultiFieldIdxList.size() -1) {
									//context.write(new Text(result), NullWritable.get());
									context.write(NullWritable.get(), new Text(result));
									result = singleField;																					
								}									
							}						
						}
					}else {
						for(String group : multiGroups){
							String[] fields1;
							if(multiFieldDelimiter.equals("|")){
								fields1 = group.split("\\|");			
							}else {
								fields1 = group.split(multiFieldDelimiter);
							}
							String result =  singleField;									
							for (int index : reservedMultiFieldIdxList) {
								result += resultFieldDelimiter + fields1[index];
							}
							//context.write(new Text(result), NullWritable.get());		
							context.write(NullWritable.get(), new Text(result));		
						}			
					}
				}
			}
	}else {//!singlefirst
			if(multiGroupDelimiter.equals(SingleMultiFieldDelimiter)){
				if (multiGroupContainsFieldNum == 1) {
					for (int i = 0; i < fields.length; i++) {								
						if(!adjustedMultiUnrelatedFieldIdxList.contains(i)) {																				
							//context.write(new Text( singleField + resultFieldDelimiter + fields[i]), NullWritable.get());	
							context.write(NullWritable.get(), new Text( fields[i] + resultFieldDelimiter + singleField ));	
						}							
					}					
				}else {
					if (multiFieldDelimiter.equals(multiGroupDelimiter)) {
						int counter = 0;
						String result = "";
						for (int i = 0; i < fields.length; i++) {								
							if(!adjustedMultiUnrelatedFieldIdxList.contains(i)) {
								int index = (counter++) % multiGroupContainsFieldNum;
								if (reservedMultiFieldIdxList.contains(index)) {										
									result += fields[i] + resultFieldDelimiter;	
									if (index == reservedMultiFieldIdxList.size() -1) {
										//context.write(new Text(result + singleField), NullWritable.get());
										context.write( NullWritable.get(), new Text(result + singleField));
										result = "";																					
									}								
								}
							}							
						}
					}else {
						for (int i = 0; i < fields.length; i++) {
							if(!adjustedMultiUnrelatedFieldIdxList.contains(i)) {													
								String[] fields1;
								if(multiFieldDelimiter.equals("|")){
									fields1 = fields[i].split("\\|");			
								}else {
									fields1 = fields[i].split(multiFieldDelimiter);
								}
								String result = "";									
								for (int index : reservedMultiFieldIdxList) {
									result += fields1[index] + resultFieldDelimiter ;
								}
								//context.write(new Text(result + singleField), NullWritable.get());
								context.write(NullWritable.get(), new Text(result + singleField));
							}							
						}							
					}
				}
			}else {//!multiGroupDelimter.equals(SingleMultiFieldDelimiter)，此时就不需要adjustedMultiUnrelatedFieldIdxList这个变量
				
				String[] multiGroups = null;				
				if(multiGroupDelimiter.equals("|")){
					multiGroups = fields[multiGroupSetIdx].split("\\|");			
				}else {
					multiGroups = fields[multiGroupSetIdx].split(multiGroupDelimiter);
				}
				
				if (multiGroupContainsFieldNum == 1) {
					for (int i = 0; i < multiGroups.length; i++) {																									
						//context.write(new Text( multiGroups[i] + resultFieldDelimiter  + singleField), NullWritable.get());	
						context.write(NullWritable.get(), new Text( multiGroups[i] + resultFieldDelimiter  + singleField));	
					}									
				}else {
					if (multiFieldDelimiter.equals(multiGroupDelimiter)) {//此时multiGroups就是multifield。
						int counter = 0;
						String result = "";
						for (int i = 0; i < multiGroups.length; i++) {					
							int index = (counter++) % multiGroupContainsFieldNum;
							if (reservedMultiFieldIdxList.contains(index)) {										
								result += multiGroups[i] +  resultFieldDelimiter ;	
								if (index == reservedMultiFieldIdxList.size() -1) {
									//context.write(new Text(result + singleField), NullWritable.get());
									context.write(NullWritable.get(), new Text(result + singleField));
									result = "";																					
								}									
							}						
						}
					}else {
						for(String group : multiGroups){
							String[] fields1;
							if(multiFieldDelimiter.equals("|")){
								fields1 = group.split("\\|");			
							}else {
								fields1 = group.split(multiFieldDelimiter);
							}
							String result =  "";									
							for (int index : reservedMultiFieldIdxList) {
								result += fields1[index] + resultFieldDelimiter ;
							}
							//context.write(new Text(result + singleField), NullWritable.get());
							context.write( NullWritable.get(), new Text(result + singleField));	
						}			
					}
				}
			}
		}						
	} 

	
	
	
	
	

	
	
	@Override
	protected void setup(Context context) throws java.io.IOException ,InterruptedException {
		

		
		Configuration conf = context.getConfiguration();
		
		SingleMultiFieldDelimiter = conf.get("single.multi.field.delimiter","|");
		multiGroupDelimiter = conf.get("multi.group.delimiter", "|");//此时无从定义multiFieldDelimiter，其实此时multiFieldDelimiter应该为空,即使不为空也无所谓，应为用不到
		multiFieldDelimiter = conf.get("multi.field.delimiter", "|");
		resultFieldDelimiter = conf.get("result.field.delimiter", "|");

		
		String[] tmp = conf.get("single.field.indexes","0").split(",");/**/
		for (int i = 0; i < tmp.length; i++) {
			singleFieldIdxList.add(Integer.parseInt(tmp[i]));			
		}
		
		if(multiGroupDelimiter.equals(SingleMultiFieldDelimiter)){
			tmp = conf.get("multi.unrelated.field.indexes","0").split(",");/**/
			multiUnrelatedFieldIdxList =  new ArrayList<Integer>();
			for (int i = 0; i < tmp.length; i++) {
				multiUnrelatedFieldIdxList.add(Integer.parseInt(tmp[i]));			
			}	
		}else {
			multiGroupSetIdx = conf.getInt("multi.group.set.index", 1);
		}
		
		tmp = conf.get("reserved.multi.field.indexes","0").split(",");/**/
		for (int i = 0; i < tmp.length; i++) {
			reservedMultiFieldIdxList.add(Integer.parseInt(tmp[i]));			
		}
		
		
		multiGroupContainsFieldNum = conf.getInt("multi.group.contains.field.number", 1);		

		singleFirst = conf.getBoolean("single.first", true);		
	}

}
