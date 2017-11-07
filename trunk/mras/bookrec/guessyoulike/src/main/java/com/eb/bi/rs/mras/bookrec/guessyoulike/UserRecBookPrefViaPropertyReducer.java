package com.eb.bi.rs.mras.bookrec.guessyoulike;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.eb.bi.rs.mras.bookrec.guessyoulike.util.TextPair;


public class UserRecBookPrefViaPropertyReducer extends Reducer<TextPair, TextPair, NullWritable, Text> {
	
	Map<String, ArrayList<Double>> bookPropertyMap = new HashMap<String, ArrayList<Double>>();

	private Map<String, String> bookClassMap;
	private ArrayList<Integer> selectPropertyIdxArr;
	private int classPrefIdx;
	
	
	enum UserPreferenceMalFormed {
		MISSINGFIELD,
		CLASSTYEMALFORMED		
	}

	
	@Override
	protected void reduce(TextPair key, Iterable<TextPair> values, Context context) throws IOException ,InterruptedException {
		//用户|属性1|属性2|。。。。|属性n
		//用户|待推图书|源图书集|预测偏好向量
		
		String userId = key.getFirst().toString();
		ArrayList<Double> propertyPrefArr = null;
		int count = 0;
		
		String userClassPref = null;   /*classid:score,classid:score,classid:score*/
		for (TextPair pair : values) {			
			if (pair.getSecond().toString().equals("0")) {
				++count;
				propertyPrefArr = new ArrayList<Double>();
				String[] fields = pair.getFirst().toString().split("\\|");
				
				try {
					if (selectPropertyIdxArr != null) {						
						for (int selectPropertyIdx : selectPropertyIdxArr) {
							propertyPrefArr.add(Double.parseDouble(fields[selectPropertyIdx]));//可能出现异常。					
						}
					}else {
						for (int idx = 1; idx < fields.length; idx++) {
							if (idx != classPrefIdx) {
								propertyPrefArr.add(Double.parseDouble(fields[idx]));
							}							
						}						
					}					
					if (classPrefIdx >= 0) {
						userClassPref = fields[classPrefIdx];
					}				
					
				} catch (Exception e) {
					context.getCounter(UserPreferenceMalFormed.MISSINGFIELD).increment(1);
					--count;
				}
			
				
					
			}else {
				StringBuffer sb = new StringBuffer(userId + "|" + pair.getFirst().toString() + "|");
				if (count != 1) {
					handleAbnormalData(sb, context);
				} else {					
					String[] fields = pair.getFirst().toString().split("\\|");
					String bookId = fields[0];	
					
					if (bookPropertyMap.containsKey(bookId)) {
						ArrayList<Double> bookPropertyArr = bookPropertyMap.get(bookId);
						if(propertyPrefArr.size() != bookPropertyArr.size()) {			
							handleAbnormalData(sb, context);							
						} else {
							for (int idx = 0; idx < propertyPrefArr.size(); idx++) {
								sb.append(propertyPrefArr.get(idx) * bookPropertyArr.get(idx) + ",");
							}							
							if (classPrefIdx < 0) {
								sb.deleteCharAt(sb.length() -1);
							} else {
								String classId = bookClassMap.get(bookId);							
								String[] classPrefPairs = userClassPref.split(",");/*format:classid:score,classid:score,classid:score*/
								boolean flag = false;
								try {
									for (String prefPair : classPrefPairs) {
										String[] classPref = prefPair.split(":");								
										if (classPref[0].equals(classId)) {
											sb.append(classPref[1]);
											flag = true;
											break;
										}
									}
									if (!flag) {
										sb.append("0");
									}					
									
								} catch (Exception e) {
									context.getCounter(UserPreferenceMalFormed.CLASSTYEMALFORMED).increment(1);
								}
									
							}
							//用户|待推荐图书|源图书集合|相似度|属性偏好
							context.write(NullWritable.get(), new Text(sb.toString()));							
						}						
						
					}else {//!如果没有该图书的图书属性信息，例如图书下架了。(后来出现了改动，保证输入不会出现下架的图书)					
						handleAbnormalData(sb, context);					
					}
				
				}//else
					
			}
			
		}

	}
	
	private void handleAbnormalData(StringBuffer sb, Context context) throws IOException, InterruptedException {
		
		for (int idx = 0; idx < selectPropertyIdxArr.size(); idx++) {
			sb.append("0," );
		}
		if (classPrefIdx < 0) {
			sb.deleteCharAt(sb.length() -1);
		}else {
			sb.append("0");										
		}
		context.write(NullWritable.get(), new Text(sb.toString()));	
		
	}
	
	
	@Override
	protected void setup(Context context) throws java.io.IOException ,InterruptedException {
		Configuration conf =  context.getConfiguration();
	
		
		String selectPropertiesStr = conf.get("select.property.indexes");
		if (selectPropertiesStr != null) {//不配置默认为全部
			selectPropertyIdxArr = new ArrayList<Integer>();
			String[] selectPropertyIdxStrArr = selectPropertiesStr.split(",");
			for (String index : selectPropertyIdxStrArr) {
				selectPropertyIdxArr.add(Integer.parseInt(index));
			}		
		}
		
		//如果没有配置为-1，
		classPrefIdx = conf.getInt("class.pref.index", -1);
		if (classPrefIdx >= 0 ) {
			bookClassMap = new HashMap<String, String>();
		}	
		
		
		
		Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);		
		for(int i = 0; i < localFiles.length; ++i) {
			String line;
			BufferedReader in = null;
			try {
				in = new BufferedReader(new FileReader(localFiles[i].toString()));
				while((line = in.readLine()) != null) {/*图书ID|属性1|属性2|xxxx|*/						
					String[] fields = line.split("\\|", -1);
					ArrayList<Double> bookPropertyArr = new ArrayList<Double>();
					if (selectPropertyIdxArr != null) {						
						for (int selectPropertyIdx : selectPropertyIdxArr) {
							bookPropertyArr.add(Double.parseDouble(fields[selectPropertyIdx]));	
						}
					}else {
						for (int j = 1; j < fields.length; j++) {
							if (j != classPrefIdx) {
								bookPropertyArr.add(Double.parseDouble(fields[j]));
							}							
						}						
					}					
					bookPropertyMap.put(fields[0], bookPropertyArr);
					if (classPrefIdx >= 0) {
						bookClassMap.put(fields[0], fields[classPrefIdx]);
					}
				}			
			}finally {
				if(in != null){
					in.close();
				}
			}			
		}	
	}
}
