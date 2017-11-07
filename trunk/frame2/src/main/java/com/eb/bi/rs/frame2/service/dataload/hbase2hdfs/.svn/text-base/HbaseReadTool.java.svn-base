package com.eb.bi.rs.frame2.service.dataload.hbase2hdfs;

import java.io.IOException;
import java.net.URI;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.client.Scan;

import com.eb.bi.rs.frame2.recframe.base.BaseDriver;


public class HbaseReadTool extends BaseDriver {

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create(getConf());

		String zkHost = properties.getProperty("conf.zk.host");
		conf.set("hbase.zookeeper.quorum", zkHost);
		String zkPort = properties.getProperty("conf.zk.port");
		conf.set("hbase.zookeeper.property.clientPort", zkPort);
		// hbase表名
		String tableName = properties.getProperty("conf.hbase.table");
		// 导出表达式的分隔符
		conf.set("conf.export.split", properties.getProperty("conf.export.split"));
		// 导出表达式
		conf.set("conf.export.express", properties.getProperty("conf.export.express"));
		// 导出时间，所有或前一天
		String time = properties.getProperty("conf.export.time");

		Job job = new Job(conf, "HbaseReader");
		job.setNumReduceTasks(0);
		job.setJarByClass(HbaseReadTool.class);
		Scan scan = new Scan();
		scan.setStartRow(HConstants.EMPTY_START_ROW);
		scan.setStopRow(HConstants.EMPTY_END_ROW);
		if (!time.equals("all")) {
			// 若不是导全表，则导昨天的数据
			scan.setTimeRange(getStartTime(), getEndTime());		
		}
		TableMapReduceUtil.initTableMapperJob(tableName, scan,  
                HbaseReadMapper.class, Text.class, NullWritable.class, job);
		
		String outputPath = properties.getProperty("conf.output.path");
		check(outputPath);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		boolean ret = job.waitForCompletion(true);

		return ret ? 0 : 1;
	}
	
	public void check(String fileName) {
		try {
			FileSystem fs = FileSystem.get(URI.create(fileName), new Configuration());
			Path f = new Path(fileName);
			if (fs.exists(f)) {
				fs.delete(f, true);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private Long getStartTime(){  
        Calendar todayStart = Calendar.getInstance();  
        todayStart.set(Calendar.HOUR, 0);  
        todayStart.set(Calendar.MINUTE, 0);  
        todayStart.set(Calendar.SECOND, 0);  
        todayStart.set(Calendar.MILLISECOND, 0);  
        todayStart.add(Calendar.DATE, -1);
        return todayStart.getTime().getTime();  
    }  
      
    private Long getEndTime(){  
        Calendar todayEnd = Calendar.getInstance();  
        todayEnd.set(Calendar.HOUR, 24);  
        todayEnd.set(Calendar.MINUTE, 0);  
        todayEnd.set(Calendar.SECOND, 0);  
        todayEnd.set(Calendar.MILLISECOND, 0);  
        todayEnd.add(Calendar.DATE, -1);
        return todayEnd.getTime().getTime();
    }  
}
