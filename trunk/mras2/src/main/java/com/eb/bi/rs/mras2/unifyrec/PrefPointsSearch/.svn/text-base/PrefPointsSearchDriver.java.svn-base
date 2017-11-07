
package com.eb.bi.rs.mras2.unifyrec.PrefPointsSearch;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import com.eb.bi.rs.mras2.unifyrec.greylist.utils.BaseDriver;
import com.eb.bi.rs.mras2.unifyrec.greylist.utils.ComponentHelper;
import com.eb.bi.rs.mras2.unifyrec.greylist.utils.JobComponent;
import com.eb.bi.rs.mras2.unifyrec.greylist.utils.PluginConfig;
import com.eb.bi.rs.mras2.unifyrec.greylist.utils.PluginExitCode;
import com.eb.bi.rs.mras2.unifyrec.greylist.utils.PluginResult;
import com.eb.bi.rs.mras2.unifyrec.greylist.utils.PluginUtil;

public class PrefPointsSearchDriver extends BaseDriver {

	public static void main(String[] args) throws Exception {
		PluginUtil pluginUtil = PluginUtil.getInstance();
		pluginUtil.init(args);
		Logger log = pluginUtil.getLogger();

		PluginConfig pluginConfig = pluginUtil.getConfig();
		JobComponent root = ComponentHelper.createComposite(pluginConfig.getElement("composite"));

		Date begin = new Date();
		int ret = root.run(null);
		Date end = new Date();
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
		String endTime = format.format(end);
		long timeCost = end.getTime() - begin.getTime();

		PluginResult result = pluginUtil.getResult();
		result.setParam("endTime", endTime);
		result.setParam("timeCosts", timeCost);
		result.setParam("exitCode", ret == 0 ? PluginExitCode.PE_SUCC
				: PluginExitCode.PE_LOGIC_ERR);
		result.setParam("exitDesc", ret == 0 ? "run successfully"
				: "run failed.");
		result.save();

		log.info("time cost in total(s): " + (timeCost / 1000.0));
		System.exit(ret);
	}

	
	@SuppressWarnings("deprecation")
	@Override
	public int run(String[] args) throws Exception {
		PluginUtil pluginUtil = PluginUtil.getInstance();
		pluginUtil.init(args);
		Logger log = pluginUtil.getLogger();

		Configuration conf = new Configuration(getConf());
   
		Properties app_conf = super.properties;
		String bookinfoPath=app_conf.getProperty("conf.bookinfo.input.path");
		String classifierPath=app_conf.getProperty("conf.classifier.input.path");
		
		conf.set("conf.search.data",properties.getProperty("conf.search.data"));
		
		conf.set("conf.userPref.output.path",properties.getProperty("conf.userPref.output.path"));
		conf.set("conf.userBookid.output.path",properties.getProperty("conf.userBookid.output.path"));
		conf.set("conf.userReadHistory.output.path",properties.getProperty("conf.userReadHistory.output.path"));
		conf.set("conf.recomBookinfo.output.path",properties.getProperty("conf.recomBookinfo.output.path"));
		conf.set("conf.classifier.output.path",properties.getProperty("conf.classifier.output.path"));
		
		//加载图书信息
		FileSystem fs1 = FileSystem.get(conf);	
		FileStatus[] status1 = fs1.globStatus(new Path (classifierPath));
		for(int i = 0;  i  < status1.length; i++){
			DistributedCache.addCacheFile(URI.create(status1[i].getPath().toString()),conf);
			log.info("classifier_output_d file: " + status1[i].getPath().toString() + " has been add into distributed cache");
		}
		FileStatus[] status2 = fs1.globStatus(new Path (bookinfoPath));
		for(int i = 0;  i  < status2.length; i++){
			DistributedCache.addCacheFile(URI.create(status2[i].getPath().toString()),conf);	
			log.info("recom_bookinfo file: " + status2[i].getPath().toString() + " has been add into distributed cache");
		}
		
		Job job = Job.getInstance(conf, "PrefPointsSearch");
	
		job.setJarByClass(PrefPointsSearchDriver.class);
		job.setMapperClass(PrefPointsSearchMapper.class);
		job.setReducerClass(PrefPointsSearchReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		String inputPath = properties.getProperty("conf.input.path");
		String [] inputPaths=inputPath.split("\\,",-1);
		StringBuffer paths=new StringBuffer();
		
		for (int i=0;i<inputPaths.length;i++){
			paths.append(inputPaths[i]);
			paths.append(",");
		}
		paths.deleteCharAt(paths.length()-1);
		String outputPath = properties.getProperty("conf.output.path");
		check(outputPath, conf);
		
		int reduceNum = Integer.parseInt(properties.getProperty("conf.num.reduce.tasks"));
		job.setNumReduceTasks(reduceNum);
		
		FileInputFormat.setInputPaths(job, paths.toString());
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		MultipleOutputs.addNamedOutput(job, "userPrefList", TextOutputFormat.class,
				Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "userBookidList", TextOutputFormat.class,
				Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "userReadHistoryList", TextOutputFormat.class,
				Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "recomBookinfo", TextOutputFormat.class,
				Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "classifier", TextOutputFormat.class,
				Text.class, NullWritable.class);
		
		boolean ret = job.waitForCompletion(true);
		return ret ? 0 : 1;
	}

	public void check(String path, Configuration conf) {
		try {
			FileSystem fs = FileSystem.get(conf);
			fs.deleteOnExit(new Path(path));
			fs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


}
