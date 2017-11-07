package com.eb.bi.rs.frame2.algorithm.occurrence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class CooccurrenceMatrixManager {
	
	public static int Vertical2horizontal(String[] args,Logger log,Configuration conf,String inpath,String outpath,int reduceNum,int mapSize) throws Exception{//输入，输出，map大小，reduce个数，邻居数，邻居分割符，键值分割符
		
		conf.set("mapred.max.split.size", String.valueOf(1024 * 1024 * mapSize));
		
		return ToolRunner.run(new Configuration(), new Vertical2horizontalDriver(log,inpath,outpath,conf,reduceNum), args);
	}
	
	public static int Cooccurrence(String[] args,Logger log,Configuration conf,String inpath,String outpath,int reduceNum,int mapSize) throws Exception{//输入，输出，map大小，reduce个数，邻居数，邻居分割符，键值分割符
		
		conf.set("mapred.max.split.size", String.valueOf(1024 * 1024 * mapSize));
		
		return ToolRunner.run(new Configuration(), new CooccurrenceDriver(log,inpath,outpath,conf,reduceNum), args);
	}
	
	public static int Frequency(String[] args,Logger log,Configuration conf,String inpath,String outpath,int reduceNum,int mapSize) throws Exception{
		
		conf.set("mapred.max.split.size", String.valueOf(1024 * 1024 * mapSize));
		
		return ToolRunner.run(new Configuration(), new FrequencyDriver(log,inpath,outpath,conf,reduceNum), args);
	}
	
	public static int OutputMatrix(String[] args,Logger log,Configuration conf,String inpath,String outpath,int reduceNum,int mapSize) throws Exception{
		
		conf.set("mapred.max.split.size", String.valueOf(1024 * 1024 * mapSize));
		
		return ToolRunner.run(new Configuration(), new OutputMatrixDriver(log,inpath,outpath,conf,reduceNum), args);
	}
	
	public static int ProportionCompute(String[] args,Logger log,Configuration conf,String inpath,String outpath,int reduceNum,int mapSize) throws Exception{
		
		conf.set("mapred.max.split.size", String.valueOf(1024 * 1024 * mapSize));
		
		return ToolRunner.run(new Configuration(), new ProportionComputeDriver(log,inpath,outpath,conf,reduceNum), args);
	}
	
	/*
	public int SubDimension(){
		
		return 0;
	}
	*/
}
