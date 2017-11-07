package com.eb.bi.rs.qhll.userapprec.predictpref;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.eb.bi.rs.frame.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame.common.pluginutil.PluginUtil;
import com.eb.bi.rs.qhll.userapprec.predictpref.ItemBaseCfInMahout;
import com.eb.bi.rs.qhll.userapprec.predictpref.ItemConvertMapper;
import com.eb.bi.rs.qhll.userapprec.predictpref.ItemConvertReducer;
import com.eb.bi.rs.qhll.userapprec.predictpref.ItemNameReplaceMapper;

public class ItemBaseCfDriver extends Configured implements Tool
{
	private static Logger log = Logger.getLogger(ItemBaseCfDriver.class);
	long start;
	
	@Override
	public int run(String[] args) throws Exception 
	{     
		String HDFS = args[0];
		JobConf conf = new JobConf();
		HdfsDAO hdfs = new HdfsDAO(HDFS, conf);
		hdfs.rmr(args[1]);
		hdfs.rmr(args[2]);
		hdfs.rmr(args[3]);
		hdfs.rmr(args[4]);
		hdfs.rmr(args[5]);
		
		/* 1. ��ȡ����itemname�������������ֽ���ӳ�� */
		/*output data format
		 *   269     U00011-03
		 */
		log.info("\n=====================1. job start to map itemname to itemid=====================");
		start = System.currentTimeMillis();
		Configuration conf1 = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
		Job convertJob = new Job(conf1, "convert item name to id");
		convertJob.setJarByClass(ItemBaseCfDriver.class);
		convertJob.setMapperClass(ItemConvertMapper.class);
		convertJob.setReducerClass(ItemConvertReducer.class);
		convertJob.setMapOutputKeyClass(Text.class);
		convertJob.setMapOutputValueClass(Text.class);
		convertJob.setOutputKeyClass(IntWritable.class);
		convertJob.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(convertJob, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(convertJob, new Path(otherArgs[2]));
		
		if( convertJob.waitForCompletion(true))
		{
			log.info("job complete, time consumed(ms): " + (System.currentTimeMillis() - start));
		}
		else 
		{
			log.error("job failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}
		
		
		/* 2. �滻�û������ļ��е�itemnameΪitemid */
		/* input data format
		 *   18897390050|U00010|0.00301325
		 * output data format
		 *   18897390050	270	  0.00301325
		 */
		log.info("\n=====================2. job start to replace itemname to itemid=====================");
		start = System.currentTimeMillis();
		Configuration conf2 = new Configuration();
		//conf2.set("mapred.job.tracker", "10.1.1.240:9001");		
		String[] otherArgs2 = new GenericOptionsParser(conf2, args).getRemainingArgs();
		String uri = otherArgs2[1] + "/part-r-00000";
		DistributedCache.addCacheFile(new URI(uri), conf2); 
		log.info(uri + " has been add to cache");		

		Job replaceJob = new Job(conf2, "replace item name to id");
		replaceJob.setJarByClass(ItemBaseCfDriver.class);
		replaceJob.setMapperClass(ItemNameReplaceMapper.class);
		replaceJob.setNumReduceTasks(0);
		replaceJob.setMapOutputKeyClass(Text.class);
		replaceJob.setMapOutputValueClass(Text.class);
		replaceJob.setOutputKeyClass(Text.class);
		replaceJob.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(replaceJob, new Path(otherArgs2[1]));
		FileOutputFormat.setOutputPath(replaceJob, new Path(otherArgs2[3]));
		if( replaceJob.waitForCompletion(true))
		{
			log.info("job complete, time consumed(ms): " + (System.currentTimeMillis() - start));
		}
		else 
		{
			log.error("job failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}
		System.out.println("\n============================================================================");

		
		
		/* 3. ����mahout�е�Эͬ�����㷨���м��� */
		log.info("\n=====================3. mahout itemCF job start =====================");
		start = System.currentTimeMillis();
		ItemBaseCfInMahout mahoutItemCF = new ItemBaseCfInMahout();
		mahoutItemCF.runMahout(args);
		log.info("job complete, time consumed(ms): " + (System.currentTimeMillis() - start));
		System.out.println("\n===========================================================");
		
		/* 4. �滻�û������ļ��е�itemidΪitemname������Ҫ��ı������ʽ */
		log.info("\n=====================4. job start to replace itemid to itemname ======================");
		start = System.currentTimeMillis();
		Configuration conf4 = new Configuration();
		String[] otherArgs4 = new GenericOptionsParser(conf4, args).getRemainingArgs();
		DistributedCache.addCacheFile(new URI(uri), conf4); 
		log.info(uri + " has been add to cache.");

		Job reReplaceJob = new Job(conf4, "replace item name to id");
		reReplaceJob.setJarByClass(ItemBaseCfDriver.class);
		reReplaceJob.setMapperClass(ItemIdReplaceMapper.class);
		reReplaceJob.setNumReduceTasks(0);
		reReplaceJob.setMapOutputKeyClass(Text.class);
		reReplaceJob.setMapOutputValueClass(Text.class);
		reReplaceJob.setOutputKeyClass(Text.class);
		reReplaceJob.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(reReplaceJob, new Path(otherArgs4[4]));
		FileOutputFormat.setOutputPath(reReplaceJob, new Path(otherArgs4[6]));
		
		if( reReplaceJob.waitForCompletion(true))
		{
			log.info("job complete, time consumed(ms): " + (System.currentTimeMillis() - start));
		}
		else 
		{
			log.error("job failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}
		System.out.println("\n============================================================================");
		
		return 0;
	}
}
