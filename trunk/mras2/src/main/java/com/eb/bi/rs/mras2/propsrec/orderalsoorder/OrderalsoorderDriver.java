package com.eb.bi.rs.mras2.propsrec.orderalsoorder;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.eb.bi.rs.frame2.algorithm.occurrence.CooccurrenceCombiner;
import com.eb.bi.rs.frame2.algorithm.occurrence.CooccurrenceMapper;
import com.eb.bi.rs.frame2.algorithm.occurrence.CooccurrenceReducer;
import com.eb.bi.rs.frame2.algorithm.occurrence.Vertical2horizontalMapper;
import com.eb.bi.rs.frame2.algorithm.occurrence.Vertical2horizontalReducer;
import com.eb.bi.rs.frame2.common.hadoop.fileopt.GetMergeFromHdfs;
import com.eb.bi.rs.frame2.common.hadoop.fileopt.PutMergeToHdfs;
import com.eb.bi.rs.frame2.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame2.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame2.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame2.common.pluginutil.PluginUtil;
import com.eb.bi.rs.mras2.propsrec.orderalsoorder.GettopNMapper;
import com.eb.bi.rs.mras2.propsrec.orderalsoorder.GettopNReducer;
import com.eb.bi.rs.mras2.propsrec.orderalsoorder.OrderalsoorderDriver;

public class OrderalsoorderDriver extends Configured  implements Tool{
	private static PluginUtil pluginUtil;
	private static Logger log;
	
	
	public OrderalsoorderDriver(String[] args){
		pluginUtil = PluginUtil.getInstance();
		pluginUtil.init(args);
		log = pluginUtil.getLogger();
	}
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		//配置加载
		Date dateBeg = new Date();
		
		int ret = ToolRunner.run(new Configuration(), new OrderalsoorderDriver(args), args);
		
		Date dateEnd = new Date();
		SimpleDateFormat format	 = new SimpleDateFormat("yyyyMMddHHmmss");
		String endTime = format.format(dateEnd);		
		long timeCost = dateEnd.getTime() - dateBeg.getTime();
		
		/*
		Properties properties = new Properties();
		InputStream inputStream = Object.class.getResourceAsStream("/pom.properties");
		properties.load(inputStream);
		System.out.println(properties.get("nam"));
		*/
		
		PluginResult result = pluginUtil.getResult();		
		result.setParam("endTime", endTime);
		result.setParam("timeCosts", timeCost);
		result.setParam("exitCode", ret == 0 ? PluginExitCode.PE_SUCC : PluginExitCode.PE_LOGIC_ERR);
		result.setParam("exitDesc", ret == 0 ? "run successfully" : "run failed.");
		result.save();
		
		log.info("time cost in total(ms) :" + timeCost) ;
		
		System.exit(ret);
		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		PluginConfig config = pluginUtil.getConfig();
		Configuration conf;
		Job job;
		long start;
		//配置加载=============================================================================
		int neighbourNum = config.getParam("neighbour_num", 0);
		
		int reduceNum1 = config.getParam("reduce_num_job1", 1);
		int reduceNum2 = config.getParam("reduce_num_job2", 1);
		int reduceNum3 = config.getParam("reduce_num_job3", 1);
		
		int maxSplitSizejob1 = config.getParam("tagdict_max_split_size_job1", 64);
		int maxSplitSizejob2 = config.getParam("tagdict_max_split_size_job2", 64);
		int maxSplitSizejob3 = config.getParam("tagdict_max_split_size_job3", 64);
		
		String hdfsworkdir = config.getParam("hdfs_work_path", "");
		
		String inputfiledir = config.getParam("input_file_path", "");
		String outputfiledir = config.getParam("output_file_path", "");
		
		String hadoopIp = config.getParam("hadoop_ip", "");
		String outputhdfsdir1 = config.getParam("intput_hdfs_path", "");//=inputhdfsdir
		String outputhdfsdir2 = config.getParam("hdfs_middle_data_path", "");
		String outputhdfsdir3 = config.getParam("hdfs_matrix_path", "");
		String outputhdfsdir4 = config.getParam("outtput_hdfs_path", "");
		
		String k_v_separator = config.getParam("k_v_separator", "");
		String id_id_separator = config.getParam("id_id_separator", "");
		String id_num_separator = config.getParam("id_num_separator", "");
		String out_Which = config.getParam("out_Which", "1");
		String top_num = config.getParam("top_num", "");
		//数据准备：导入HDFS=====================================================================
		//PutMergeToHdfs putdata2Hdfs = new PutMergeToHdfs();
		//putdata2Hdfs.put(inputfiledir, hadoopIp, outputhdfsdir1);
		//==================================================================================
		start = System.currentTimeMillis();
		//==================================================================================
		conf = new Configuration(getConf());
		
		conf.set("mapred.max.split.size", String.valueOf(1024 * 1024 * maxSplitSizejob1));
		
		conf.set("neighbour_num", String.valueOf(neighbourNum));
		
		conf.set("max_item_num", "5000");
		
		//conf.set("k_v_separator", k_v_separator);
		conf.set("id_id_separator", id_id_separator);
		conf.set("id_num_separator", id_num_separator);
		conf.set("out_Which", out_Which);
		conf.set("top_num", top_num);
		
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", k_v_separator);
		conf.set("mapred.textoutputformat.separator",k_v_separator);
		
		rmr(hdfsworkdir,conf);
		
		//数据准备：导入HDFS=====================================================================
		PutMergeToHdfs putdata2Hdfs = new PutMergeToHdfs();
		putdata2Hdfs.put(inputfiledir, hadoopIp, outputhdfsdir1);
		//竖表转横表job===============================================================
		job = new Job(conf);
		job.setJarByClass(OrderalsoorderDriver.class);
		
		//设置输入地址
		FileInputFormat.setInputPaths(job, new Path(outputhdfsdir1));
		//设置输出地址
		FileOutputFormat.setOutputPath(job, new Path(outputhdfsdir2));
		
		//设置M-R
		job.setMapperClass(Vertical2horizontalMapper.class);
		job.setNumReduceTasks(reduceNum1);
		job.setReducerClass(Vertical2horizontalReducer.class);
		
		//设置输入/输出格式
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//日志==================================================================================
		if( job.waitForCompletion(true)){
			log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
		}
		else {
			log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}
		log.info("=================================================================================");	
		//共现矩阵计算job==================================================================================
		conf.set("mapred.max.split.size", String.valueOf(1024 * 1024 * maxSplitSizejob2));
		
		job = new Job(conf);
		job.setJarByClass(OrderalsoorderDriver.class);
		
		//设置输入地址
		FileInputFormat.setInputPaths(job, new Path(outputhdfsdir2 + "/part-*"));
		//设置输出地址
		FileOutputFormat.setOutputPath(job, new Path(outputhdfsdir3));
		
		//设置M-R
		job.setMapperClass(CooccurrenceMapper.class);
		job.setCombinerClass(CooccurrenceCombiner.class);
		job.setNumReduceTasks(reduceNum2);
		job.setReducerClass(CooccurrenceReducer.class);
		
		//设置输入/输出格式
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//日志==================================================================================
		if( job.waitForCompletion(true)){
			log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
		}
		else {
			log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}
		log.info("=================================================================================");	
		//topN截取job========================================================================
		conf.set("mapred.max.split.size", String.valueOf(1024 * 1024 * maxSplitSizejob3));
		
		job = new Job(conf);
		job.setJarByClass(OrderalsoorderDriver.class);
		
		//设置输入地址
		FileInputFormat.setInputPaths(job, new Path(outputhdfsdir3 + "/part-*"));
		//设置输出地址
		FileOutputFormat.setOutputPath(job, new Path(outputhdfsdir4));
		
		//设置M-R
		job.setMapperClass(GettopNMapper.class);
		job.setNumReduceTasks(reduceNum3);
		job.setReducerClass(GettopNReducer.class);
		
		//设置输入/输出格式
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//日志==================================================================================
		if( job.waitForCompletion(true)){
			log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
		}
		else {
			log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}
		log.info("=================================================================================");
		//结果文件写回本地========================================================================
		GetMergeFromHdfs getfile4Hdfs = new GetMergeFromHdfs();
		getfile4Hdfs.get(hadoopIp, outputhdfsdir4 + "/part-*", outputfiledir);
		//==================================================================================
		return 0;
	}
	
	public void rmr(String folder, Configuration conf) throws IOException 
	{
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(conf);
        fs.deleteOnExit(path);
        log.info("Delete: " + folder);
        fs.close();
    }

	
	/*
	public void check(String fileName) {
		try {
			FileSystem fs = FileSystem.get(URI.create(fileName),new Configuration());
			Path f = new Path(fileName);
			boolean isExists = fs.exists(f);
			if (isExists) {	//if exists, delete
				boolean isDel = fs.delete(f,true);
				log.info(fileName + "  delete?\t" + isDel);
			} else {
				log.info(fileName + "  exist?\t" + isExists);
			}	
		} catch (IOException e) {
			e.printStackTrace();
		}	
	}
	*/
}
