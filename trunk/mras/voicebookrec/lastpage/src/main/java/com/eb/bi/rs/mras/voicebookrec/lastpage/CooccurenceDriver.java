package com.eb.bi.rs.mras.voicebookrec.lastpage;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.eb.bi.rs.frame.common.hadoop.fileopt.PutMergeToHdfs;
import com.eb.bi.rs.frame.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame.common.pluginutil.PluginUtil;
import com.eb.bi.rs.algorithm.occurrence.CooccurrenceMatrixManager;

public class CooccurenceDriver extends Configured implements Tool{

	/**
	 * @param args
	 */
	private static PluginUtil m_pluginUtil;
	private static Logger m_log;
	
    public static void main( String[] args ) throws Exception {
    	m_pluginUtil = PluginUtil.getInstance();
    	m_pluginUtil.init(args);
		m_log = m_pluginUtil.getLogger();
		
		Date dateBeg = new Date();
		
		int ret = ToolRunner.run(new CooccurenceDriver(), args);
    	
    	Date dateEnd = new Date();
    	
    	SimpleDateFormat format	 = new SimpleDateFormat("yyyyMMddHHmmss");
    	String endTime = format.format(dateEnd);		
    	long timeCost = dateEnd.getTime() - dateBeg.getTime();
    			
    	PluginResult result = m_pluginUtil.getResult();		
    	result.setParam("endTime", endTime);
    	result.setParam("timeCosts", timeCost);
    	result.setParam("exitCode", ret == 0 ? PluginExitCode.PE_SUCC : PluginExitCode.PE_LOGIC_ERR);
    	result.setParam("exitDesc", ret == 0 ? "run successfully" : "run failed.");
    	result.save();
    			
    	m_log.info("time cost in total(ms) :" + timeCost) ;
    			
    	System.exit(ret);
    }
    

    
    public static void rmr(String folder, Configuration conf) throws IOException 
	{
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(conf);
        fs.deleteOnExit(path);
        m_log.info("Delete: " + folder);
        fs.close();
    }

	@Override
	public int run(String[] args) throws Exception {
		PluginConfig config  = m_pluginUtil.getConfig();
    	Configuration m_conf = new Configuration(getConf());
    	
    	int reduceNum = config.getParam("reduce_num_job", 1);
    	
    	int mapSize1 = config.getParam("tagdict_max_split_size_job1", 64);
    	int mapSize2 = config.getParam("tagdict_max_split_size_job2", 64);
    	int mapSize3 = config.getParam("tagdict_max_split_size_job3", 64);
    	int mapSize4 = config.getParam("tagdict_max_split_size_job4", 64);

    	
    	String inputfiledir  = config.getParam("input_file_path", "");

    	
    	String hdfsworkdir = config.getParam("hdfs_work_path", "");
    	
    	String middleputhdfsdir1 = config.getParam("Hdfs_File_Input", "");
    	String middleputhdfsdir2 = config.getParam("Hdfs_Matrix_Input", "");
    	String middleputhdfsdir3 = config.getParam("Hdfs_Matrix_Result", "");
    	String middleputhdfsdir4 = config.getParam("Hdfs_Matrix_Proportion_Result", "");
    	String middleputhdfsdir6 = config.getParam("Hdfs_Item_Frequency", "");
    	
    	
    	//应用的hadoop配置
    	String k_v_separator = config.getParam("k_v_separator", "|");    	
    	int neighbourNum        = config.getParam("neighbour_num", 0);
    	String id_id_separator  = config.getParam("id_id_separator", ",");
    	String id_num_separator = config.getParam("id_num_separator", "|");
    	
    	//-------------------------------------------------------
    	m_conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", k_v_separator);
    	m_conf.set("mapred.textoutputformat.separator",k_v_separator);
    	//-------------------------------------------------------
    	m_conf.set("neighbour_num", String.valueOf(neighbourNum));
    	m_conf.set("id_id_separator", id_id_separator);
    	m_conf.set("id_num_separator", id_num_separator);
    	
    	m_conf.set("max_item_num", "5000");
    
    	//-------------------------------------------------------
    	int ret = 0;    	
    	rmr(hdfsworkdir,m_conf);    	
    	PutMergeToHdfs.put(inputfiledir, middleputhdfsdir1);
    	
    	//MR--------------------------------------------------------------------------------------
    	/*
    	 * 必要配置:args, m_log(日志)
    	 * 		 reducenum, mapsize
    	 * 		    输入,输出地址
    	 * 		 val内分割符:id_id_separator
    	 */
    	ret = ret + CooccurrenceMatrixManager.Vertical2horizontal(args, m_log, m_conf, middleputhdfsdir1, middleputhdfsdir2, reduceNum, mapSize1);
    	
    	/*
    	 * 必要配置:args, m_log(日志)
    	 * 		 reducenum, mapsize
    	 * 		    输入,输出地址
    	 * 		 val内分割符:id_id_separator
    	 * 		 val内分隔符:id_num_separator
    	 * 		 topnum:top_num
    	 */
    	ret = ret + CooccurrenceMatrixManager.Cooccurrence(args, m_log, m_conf, middleputhdfsdir2, middleputhdfsdir3, reduceNum, mapSize2);    	
    	
    	/*
    	 * 必要配置:args, m_log(日志)
    	 * 		 reducenum, mapsize
    	 * 		    输入,输出地址
    	 * 		 val内分隔符:id_num_separator
    	 */
    	ret = ret + CooccurrenceMatrixManager.ProportionCompute(args, m_log, m_conf, middleputhdfsdir3, middleputhdfsdir4, reduceNum, mapSize4);
    	
    	/*
    	 * 必要配置:args, m_log(日志)
    	 * 		 reducenum, mapsize
    	 * 		    输入,输出地址
    	 */
    	ret = ret + CooccurrenceMatrixManager.Frequency(args, m_log, m_conf, middleputhdfsdir1, middleputhdfsdir6, reduceNum, mapSize3);

    	//MR--------------------------------------------------------------------------------------

    	return ret;
	}


}
