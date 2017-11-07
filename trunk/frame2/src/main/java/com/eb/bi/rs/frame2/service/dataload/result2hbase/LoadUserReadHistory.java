package com.eb.bi.rs.frame2.service.dataload.result2hbase;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.eb.bi.rs.frame2.recframe.base.BaseDriver;
import com.ebupt.hbase.dao.TableVo;
import com.ebupt.hbase.dao.TableDao;
import com.ebupt.hbase.dao.DBConnection;

public class LoadUserReadHistory extends BaseDriver {

    private static final String CF = "cf";

    public static class LoadUserReadHistoryMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {

        private static final byte[] ONE = Bytes.toBytes("1");

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\\|");
            if (fields.length == 2) {
                String user = fields[0];
                int len = user.length();
                if (len < 4) {
                    return;
                }
                byte[] rowKey = Bytes.toBytes(user.substring(len - 4, len - 2) + user);
                ImmutableBytesWritable imRowKey = new ImmutableBytesWritable(rowKey);
                context.write(imRowKey, new KeyValue(rowKey, Bytes.toBytes(LoadUserReadHistory.CF), Bytes.toBytes(fields[1]), ONE));
            }
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();

        String zkHost = properties.getProperty("conf.zk.host");
        conf.set("hbase.zookeeper.quorum", zkHost);
        String zkPort = properties.getProperty("conf.zk.port");
        conf.set("hbase.zookeeper.property.clientPort", zkPort);
        String tabName = properties.getProperty("conf.hbase.table");
        String tmpTabName = tabName + "_tmp";
        String split = properties.getProperty("conf.hbase.region.split");

        TableDao dao = new TableDao(new DBConnection(zkHost, zkPort));
        TableVo tmpTab = new TableVo(tmpTabName, tabName);
        tmpTab.addColumnFamily(LoadUserReadHistory.CF);
        if (split != null && !"".equals(split)) {
            //tmpTab.setSplit(split.split(","));
        }
        dao.doCreate(tmpTab);

        Job job = new Job(conf, "load-user-read-history");
        job.setMapperClass(LoadUserReadHistoryMapper.class);
        job.setJarByClass(LoadUserReadHistory.class);

        HTable table = new HTable(conf, tmpTabName);
        HFileOutputFormat2.configureIncrementalLoad(job, table);

        String inputPath = properties.getProperty("conf.input.path");
        FileInputFormat.setInputPaths(job, inputPath);
        String outputPath = "result2hbase/" + tabName + "_out_tmp";
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        check(outputPath);
        boolean ret = job.waitForCompletion(true);

        if (!ret) {
            return 1;
        }

        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
        loader.doBulkLoad(new Path(outputPath), table);

        TableVo tab = new TableVo(tabName);
        ret = ret && dao.doDelete(tab) && dao.doUpdate(tmpTab);

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
}
