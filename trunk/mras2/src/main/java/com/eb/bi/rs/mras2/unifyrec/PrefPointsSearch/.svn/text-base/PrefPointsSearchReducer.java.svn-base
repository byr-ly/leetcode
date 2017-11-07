package com.eb.bi.rs.mras2.unifyrec.PrefPointsSearch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

public class PrefPointsSearchReducer extends Reducer<Text, Text, Text, NullWritable> {
	
	private MultipleOutputs<Text, NullWritable> mos;
	private String userPrefPath = null;
	private String userBookidPath = null;
	private String userReHispath=null;
	private String recomBookinfo=null;
	private String classifierPath=null;
	private Map<String,String> bookinfoMap = new HashMap<String,String>();
	private Map<String,String> classifierMap=new HashMap<String,String>();

	@SuppressWarnings("deprecation")
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		File [] inputFiles;
		String line;
		BufferedReader br = null;
		// 初始化多输出
		mos = new MultipleOutputs<Text, NullWritable>(context);

		Configuration conf = context.getConfiguration();
		userPrefPath = conf.get("conf.userPref.output.path");
		userBookidPath = conf.get("conf.userBookid.output.path");
		userReHispath = conf.get("conf.userReadHistory.output.path");//recom_bookinfo
		recomBookinfo=conf.get("conf.recomBookinfo.output.path");
		classifierPath=conf.get("conf.classifier.output.path");
		//获取图书信息bookinfo和图书相似classifier
		super.setup(context);
		Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		System.out.println("localFiles-----"+localFiles);
		try {
			for(int i = 0; i < localFiles.length; i++){
				if (localFiles[i].toString().contains("classifier")){
						br = new BufferedReader(new FileReader(localFiles[i].toString()));
						while((line=br.readLine())!=null){
							String [] fields = line.split("\\|");
							classifierMap.put(fields[0], line);//346350018|99.966782|20170811
						}
				}else if(localFiles[i].toString().contains("bookinfo")){
					   br = new BufferedReader(new FileReader(localFiles[i].toString()));
					   while((line=br.readLine())!=null){
							String [] fields = line.split("\\|");
							bookinfoMap.put(fields[0], line);//374777415|18|4|0|1|1|0|3|0
						}
					}
				}
			 
			} finally {
				if (br != null) {
					br.close();
				}			
			}			
	}	
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {   
    	for(Text val : values){
    		String [] splits=val.toString().split("\\|",-1);
    		if(bookinfoMap.containsKey(splits[1])){
    			String value=bookinfoMap.get(splits[1]);
        		mos.write(new Text(value), NullWritable.get(),recomBookinfo);	
    		}
    		if(classifierMap.containsKey(splits[1])){
    			String value=classifierMap.get(splits[1]);
    			mos.write(new Text(value), NullWritable.get(),classifierPath);
    		}
        	if(splits.length>10 && splits[0].length()==11){
            	mos.write(new Text(val), NullWritable.get(),userPrefPath);	
            }else if(splits.length==2 ){
            	if(splits[1].length()>5){
            		mos.write(new Text(val), NullWritable.get(),userReHispath);//13007237914|369867478
            	}else{
            		mos.write(new Text(val), NullWritable.get(),userBookidPath);//12345678912|13
            	}  		
            } 	
    	}        	
     }
    @Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
	}

}
