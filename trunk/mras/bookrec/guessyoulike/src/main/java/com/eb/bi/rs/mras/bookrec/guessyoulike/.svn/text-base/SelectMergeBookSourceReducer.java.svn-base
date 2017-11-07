package com.eb.bi.rs.mras.bookrec.guessyoulike;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;




public class SelectMergeBookSourceReducer extends Reducer<Text, Text, NullWritable, Text>{
	private HashMap<String, Double> weightMap = new HashMap<String, Double>();
	private int[] selectSimilarityIdxArr;
	private int similirityCnt;
	private Text outValue = new Text();
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException ,InterruptedException {
		//key:用户|待推图书,value:源图书集|来源|预测偏好向量
		double[] similarityVecSum;
		String mergedSrcBookSet = "";
		if (selectSimilarityIdxArr == null) {
			similarityVecSum = new double[similirityCnt];
		}else {			
			similarityVecSum = new double[selectSimilarityIdxArr.length];
		}
		
		for(Text value : values){
			String[] fields = value.toString().split("\\|",-1);
			String source = fields[1];		
			if (weightMap.containsKey(source)){
				double weight = weightMap.get(source);
				String[] similarityVec = fields[2].split(",");
				if (selectSimilarityIdxArr == null) {					
					for (int idx = 0; idx < similarityVec.length; idx++) {						
						similarityVecSum[idx] +=  weight * Double.parseDouble(similarityVec[idx]);					
					}	
				}else {					
					for (int idx = 0; idx < selectSimilarityIdxArr.length; idx++) {						
						similarityVecSum[idx] +=  weight * Double.parseDouble(similarityVec[selectSimilarityIdxArr[idx]]);					
					}	
				}				
				mergedSrcBookSet += source + ":" + fields[0] + ";";  //source1:book1,book2;source2:book2,book2;
				
			}else {
				String msg = String.format("the source %s weight info does not exist", source);
				throw new RuntimeException(msg);
			}
		}
		mergedSrcBookSet = mergedSrcBookSet.substring(0, mergedSrcBookSet.length() -1);
		String similarityVecSumStr = "";		
		for (int idx = 0; idx < similarityVecSum.length; idx++) {
			if(idx == similarityVecSum.length -1){
				similarityVecSumStr += similarityVecSum[idx] ;
			}else {
				similarityVecSumStr += similarityVecSum[idx] + ",";
			}
		}
		outValue.set(key + "|" + mergedSrcBookSet + "|" + similarityVecSumStr );		
		context.write(NullWritable.get(), outValue );		
	}
	
	
	@Override
	protected void setup(Context context) throws IOException ,InterruptedException {
		
		Configuration conf = context.getConfiguration();
		
		similirityCnt = conf.getInt("similarity.preference.vector.orginal.size",1);//
		
		
		String selectSimilaritiesStr = conf.get("select.similarity.indexes");//注意：这个索引在是相似度集合中的索引，起始值为0
		if(selectSimilaritiesStr != null) {//不配置默认为全部			
			String[] selectSimilarityIdxStrArr = selectSimilaritiesStr.split(",");
			selectSimilarityIdxArr = new int[selectSimilarityIdxStrArr.length];	
			for (int idx = 0; idx < selectSimilarityIdxStrArr.length; idx++) {
				selectSimilarityIdxArr[idx] = Integer.parseInt(selectSimilarityIdxStrArr[idx]);
			}
		}		
		String sourceWeight = conf.get("source.weight");/*source1:weight1,source2:wight2*/
		if(sourceWeight == null){
			throw new RuntimeException("configuration item source.weight does not exist");
		}
		String[] pairs = sourceWeight.split(",");
		for (String pair: pairs) {
			String[] split = pair.split(":");
			weightMap.put(split[0], Double.parseDouble(split[1]));			
		}	
	}
}

