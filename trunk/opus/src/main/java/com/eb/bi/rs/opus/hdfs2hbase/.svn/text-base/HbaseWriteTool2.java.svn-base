package com.eb.bi.rs.opus.hdfs2hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import com.eb.bi.rs.opus.hdfs2hbase.base.BaseDriver;

public class HbaseWriteTool2 extends BaseDriver {

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create(getConf());

		String zkHost = properties.getProperty("conf.zk.host");
		conf.set("hbase.zookeeper.quorum", zkHost);
		String zkPort = properties.getProperty("conf.zk.port");
		conf.set("hbase.zookeeper.property.clientPort", zkPort);

		System.setProperty("java.security.krb5.kdc", zkHost);
		System.setProperty("java.security.krb5.realm", "NBDP.COM");

		conf.set("hadoop.security.authentication", "kerberos");
		conf.set("hbase.security.authorization", "true");
		conf.set("hbase.master.kerberos.principal", "hbase/_HOST@NBDP.COM");
		conf.set("hbase.regionserver.kerberos.principal",
				"hbase/_HOST@NBDP.COM");
		conf.set("hbase.thrift.kerberos.principal", "hbase/_HOST@NBDP.COM");
		conf.set("hbase.zookeeper.client.kerberos.principal",
				"zookeeper/_HOST@NBDP.COM");

		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

		UserGroupInformation.setConfiguration(conf);
		UserGroupInformation userGroupInformation;
		try {
			userGroupInformation = UserGroupInformation
					.loginUserFromKeytabAndReturnUGI("eb@NBDP.COM",
							"/data/eb/recsys/eb.keytab");
			UserGroupInformation.setLoginUser(userGroupInformation);
		} catch (IOException e) {
			e.printStackTrace();
		}

		// hbase表名
		String tableName = properties.getProperty("conf.hbase.table");
		conf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
		// 导入表达式的分隔符
		conf.set("conf.import.split",
				properties.getProperty("conf.import.split"));
		// 导出表达式
		conf.set("conf.import.express",
				properties.getProperty("conf.import.express"));
		if (Boolean.parseBoolean(properties.getProperty("conf.truncate.table",
				"false"))) {
			// 首先要获得table的建表信息
			HBaseAdmin admin = new HBaseAdmin(conf);
			HTableDescriptor td = admin.getTableDescriptor(Bytes
					.toBytes(tableName));
			// 删除表
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			// 重新建表 ""
			admin.createTable(td);
		}

		Job job = Job.getInstance(conf, "HbaseWrite"); // new Job(conf,
														// "HbaseWrite");
		job.setNumReduceTasks(0);
		job.setJarByClass(HbaseWriteTool2.class);
		job.setMapperClass(HbaseWriteMapper2.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(Writable.class);

		String inputPath = properties.getProperty("conf.input.path");
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		boolean ret = job.waitForCompletion(true);

		return ret ? 0 : 1;
	}

}
