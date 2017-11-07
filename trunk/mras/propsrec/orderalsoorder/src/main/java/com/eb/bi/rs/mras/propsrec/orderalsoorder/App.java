package com.eb.bi.rs.mras.propsrec.orderalsoorder;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.eb.bi.rs.algorithm.occurrence.Vertical2horizontalMapper;
import com.eb.bi.rs.algorithm.occurrence.Vertical2horizontalReducer;
import com.eb.bi.rs.frame.common.hadoop.fileopt.GetMergeFromHdfs;
import com.eb.bi.rs.frame.common.hadoop.fileopt.PutMergeToHdfs;
import com.eb.bi.rs.frame.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame.common.pluginutil.PluginUtil;

/**
 * Hello world!
 *
 */
public class App extends Configured  implements Tool
{
	private static PluginUtil pluginUtil;
	private static Logger log;
	
	public App(String[] args){
		pluginUtil = PluginUtil.getInstance();
		pluginUtil.init(args);
		log = pluginUtil.getLogger();
	}
	
    public static void main( String[] args ) throws Exception
    {
    	// TODO Auto-generated method stub
    			//配置加载
    			Date dateBeg = new Date();
    			
    			int ret = ToolRunner.run(new Configuration(), new App(args), args);
    			
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
    
    //MAP
    public class StripesOccurrenceMapper extends Mapper<Text,Text,Text,MapWritable> {
    	  private MapWritable occurrenceMap = new MapWritable();
    	  private Text word = new Text();

    	  @Override
    	 protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
    	   //int neighbors = context.getConfiguration().getInt("neighbors", 2);
    	   String[] tokens = value.toString().split(",");
    	   if (tokens.length > 1) {
    	      for (int i = 0; i < tokens.length; i++) {
    	          word.set(tokens[i]);
    	          occurrenceMap.clear();

    	          //int start = (i - neighbors < 0) ? 0 : i - neighbors;
    	          //int end = (i + neighbors >= tokens.length) ? tokens.length - 1 : i + neighbors;
    	           for (int j = 0; j < tokens.length; j++) {
    	                if (j == i) continue;
    	                Text neighbor = new Text(tokens[j]);
    	                if(occurrenceMap.containsKey(neighbor)){
    	                	LongWritable count = (LongWritable)occurrenceMap.get(neighbor);
    	                   count.set(count.get()+1);
    	                }else{
    	                   occurrenceMap.put(neighbor,new LongWritable(1));
    	                }
    	           }
    	          context.write(word,occurrenceMap);
    	     }
    	   }
    	  }
    	}

    //REDUCE
    public class StripesReducer extends Reducer<Text, MapWritable, Text, MapWritable> {
        private MapWritable incrementingMap = new MapWritable();

        @Override
        protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            incrementingMap.clear();
            for (MapWritable value : values) {
                addAll(value);
            }
            context.write(key, incrementingMap);
        }

        private void addAll(MapWritable mapWritable) {
            Set<Writable> keys = mapWritable.keySet();
            for (Writable key : keys) {
            	LongWritable fromCount = (LongWritable) mapWritable.get(key);
                if (incrementingMap.containsKey(key)) {
                	LongWritable count = (LongWritable) incrementingMap.get(key);
                    count.set(count.get() + fromCount.get());
                } else {
                    incrementingMap.put(key, fromCount);
                }
            }
        }
    }

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		PluginConfig config = pluginUtil.getConfig();
		Configuration conf;
		Job job;
		long start;
		//配置加载=============================================================================
		int reduceNum = Integer.valueOf(config.getParam("reduce_num", "")).intValue();
		
		String inputfiledir = config.getParam("input_file_path", "");
		String outputfiledir = config.getParam("output_file_path", "");
		
		String hadoopIp = config.getParam("hadoop_ip", "");
		String outputhdfsdir1 = config.getParam("intput_hdfs_path", "");//=inputhdfsdir
		String outputhdfsdir2 = config.getParam("hdfs_middle_data_path", "");
		String outputhdfsdir3 = config.getParam("outtput_hdfs_path", "");
		
		String k_v_separator = config.getParam("k_v_separator", "");
		String id_id_separator = config.getParam("id_id_separator", "");
		String id_num_separator = config.getParam("id_num_separator", "");
		String out_Which = config.getParam("out_Which", "1");
		String top_num = config.getParam("top_num", "");
		//数据准备：导入HDFS=====================================================================
		PutMergeToHdfs putdata2Hdfs = new PutMergeToHdfs();
		putdata2Hdfs.put(inputfiledir, hadoopIp, outputhdfsdir1);
		//==================================================================================
		start = System.currentTimeMillis();
		//==================================================================================
		conf = new Configuration();
		
		//conf.set("k_v_separator", k_v_separator);
		conf.set("id_id_separator", id_id_separator);
		conf.set("id_num_separator", id_num_separator);
		conf.set("out_Which", out_Which);
		conf.set("top_num", top_num);
		
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", k_v_separator);
		conf.set("mapred.textoutputformat.separator",k_v_separator);
		//竖表转横表job===============================================================
		job = new Job(conf);
		job.setJarByClass(App.class);
		
		//设置输入地址
		FileInputFormat.setInputPaths(job, new Path(outputhdfsdir1));
		//设置输出地址
		FileOutputFormat.setOutputPath(job, new Path(outputhdfsdir2));
		
		//设置M-R
		job.setMapperClass(Vertical2horizontalMapper.class);	
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
		job = new Job(conf);
		job.setJarByClass(App.class);
		
		//设置输入地址
		FileInputFormat.setInputPaths(job, new Path(outputhdfsdir2 + "/part-*"));
		//设置输出地址
		FileOutputFormat.setOutputPath(job, new Path(outputhdfsdir3));
		
		//设置M-R
		job.setMapperClass(StripesOccurrenceMapper.class);
		//job.setCombinerClass(CooccurrenceCombiner.class);
		job.setNumReduceTasks(reduceNum);
		job.setReducerClass(StripesReducer.class);
		
		//设置输入/输出格式
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);
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
		getfile4Hdfs.get(hadoopIp, outputhdfsdir3 + "/part-*", outputfiledir);
		//==================================================================================
		return 0;
	}

}
