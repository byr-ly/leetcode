package com.eb.bi.rs.frame2.algorithm.occurrence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class ConfidenceLevelManager {
public static int ConfidenceLevel(String[] args,Logger log,Configuration conf,String inpath,String outpath,String workpath,int reduceNum,int mapSize,String KULC_threshold,String IR_threshold,String min_num,String neighbour_num, String max_item_num, String input_field_delimiter) throws Exception{//输入，输出，map大小，reduce个数，邻居数，邻居分割符，键值分割符
		
		conf.set("mapred.max.split.size", String.valueOf(1024 * 1024 * mapSize));
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", input_field_delimiter);
		conf.set("mapred.textoutputformat.separator",input_field_delimiter);
    	
    	conf.set("neighbour_num", neighbour_num);
    	
    	conf.set("max_item_num", max_item_num);
    	
    	conf.set("id_id_separator", "|");
    	conf.set("id_num_separator", "|");
    	
    	conf.set("KULC_threshold", KULC_threshold);
    	conf.set("IR_threshold", IR_threshold);
    	conf.set("min_num", min_num);
		
		int ret = 0;
		
		ret = ret + CooccurrenceMatrixManager.Vertical2horizontal(args, log, conf, inpath, workpath + "/middle", reduceNum, mapSize);
		conf.set("mapred.textoutputformat.separator","|");
		ret = ret + CooccurrenceMatrixManager.Cooccurrence(args, log, conf, workpath + "/middle/part*", workpath + "/cooccurrence", reduceNum, mapSize);		
		ret = ret + CooccurrenceMatrixManager.Frequency(args, log, conf, inpath, workpath + "/frequency", reduceNum, mapSize);		
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "|");
		ret = ret + ToolRunner.run(new Configuration(), new ConfidenceLevelDriver(log,workpath,outpath,conf,reduceNum), args);
		
		return ret;
	}
}
