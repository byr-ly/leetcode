package com.eb.bi.rs.result2hbase;

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
import com.ebupt.hbase.dao.TableVo;
import com.ebupt.hbase.dao.TableDao;
import com.ebupt.hbase.dao.DBConnection;

public class LoadUserReadHistory2 extends BaseDriver {

    private static final String CF = "cf";

    public static class LoadUserReadHistoryMapper2 extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {

        private static final byte[] ONE = Bytes.toBytes("1");
        private static final byte[] TWO = Bytes.toBytes("2");

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\\|");
            if (fields.length >= 3) {
                String user = fields[0];
                String flag = fields[2];
                int len = user.length();
                if (len < 4) {
                    return;
                }
                byte[] rowKey = Bytes.toBytes(user.substring(len - 4, len - 2) + user);
                ImmutableBytesWritable imRowKey = new ImmutableBytesWritable(rowKey);
                if(flag.equals("0")){
                	context.write(imRowKey, new KeyValue(rowKey, Bytes.toBytes(LoadUserReadHistory2.CF), Bytes.toBytes(fields[1]), ONE));
                }else{
                	context.write(imRowKey, new KeyValue(rowKey, Bytes.toBytes(LoadUserReadHistory2.CF), Bytes.toBytes(fields[1]), TWO));
                }
                
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
        tmpTab.addColumnFamily(LoadUserReadHistory2.CF);
        if (split != null && !"".equals(split)) {
            tmpTab.setSplit(split.split(","));
        }
        dao.doCreate(tmpTab);

        Job job = Job.getInstance(conf, "load-user-read-history");
        job.setMapperClass(LoadUserReadHistoryMapper2.class);
        job.setJarByClass(LoadUserReadHistory2.class);

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
