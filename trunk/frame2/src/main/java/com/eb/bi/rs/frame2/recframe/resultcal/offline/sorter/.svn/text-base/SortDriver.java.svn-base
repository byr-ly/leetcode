package com.eb.bi.rs.frame2.recframe.resultcal.offline.sorter;

import com.eb.bi.rs.frame2.recframe.base.BaseDriver;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;

public class SortDriver extends BaseDriver {

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();

        String value;
        if ((value = properties.getProperty("field.delimiter")) != null) {
            conf.set("field.delimiter", value);
        }
        if ((value = properties.getProperty("key.field.indexes")) != null) {
            conf.set("key.field.indexes", value);
        }
        if ((value = properties.getProperty("order.mode")) == null) {
            throw new RuntimeException("order mode is essential");
        } else {
            conf.set("order.mode", value);
        }

        //输入输出相关配置
        String inputPaths = properties.getProperty("input.path");
        if (inputPaths == null) {
            throw new RuntimeException("input path is essential");
        }
        String outputPath = properties.getProperty("output.path");
        if (outputPath == null) {
            throw new RuntimeException("output path is essential");
        }

        //mr配置相关
        String reduceNum = properties.getProperty("mapred.reduce.tasks");
        if (reduceNum != null) {
            conf.set("mapred.reduce.tasks", reduceNum);
        }

        if ((value = properties.getProperty("mapreduce.inputformat.class")) != null) {
            conf.setClass("mapreduce.inputformat.class", Class.forName(value), InputFormat.class);
        }
        if ((value = properties.getProperty("mapreduce.outputformat.class")) != null) {
            conf.setClass("mapreduce.outputformat.class", Class.forName(value), OutputFormat.class);
        }
        if ((value = properties.getProperty("mapred.output.compress")) != null) {
            conf.setBoolean("mapred.output.compress", Boolean.parseBoolean(value));
        }
        if ((value = properties.getProperty("mapred.output.compression.codec")) != null) {
            conf.setClass("mapred.output.compression.codec", Class.forName(value), CompressionCodec.class);
        }
        if ((value = properties.getProperty("mapred.output.compression.type")) != null) {
            conf.set("mapred.output.compression.type", value);
        }

        Job job = Job.getInstance(conf, getClass().getSimpleName());
        job.setJarByClass(getClass());

        job.setPartitionerClass(ActualKeyPartition.class);
        job.setGroupingComparatorClass(ActualKeyGroupingComparator.class);
        job.setSortComparatorClass(CompositeKeyComparator.class);

        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, inputPaths);
        check(outputPath, conf);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SortDriver(), args);
        System.exit(exitCode);
    }

    public static class ActualKeyPartition extends Partitioner<CompositeKey, Text> {
        HashPartitioner<Text, Text> hashPartitioner = new HashPartitioner<Text, Text>();
        Text actualKey = new Text();

        @Override
        public int getPartition(CompositeKey key, Text value, int numPartitions) {
            try {
                actualKey.set(key.getKey());
                return hashPartitioner.getPartition(actualKey, value, numPartitions);

            } catch (Exception e) {
                e.printStackTrace();
                return (int) (Math.random() * numPartitions);
            }
        }
    }

    public static class ActualKeyGroupingComparator extends WritableComparator {

        protected ActualKeyGroupingComparator() {
            super(CompositeKey.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            CompositeKey key1 = (CompositeKey) w1;
            CompositeKey key2 = (CompositeKey) w2;
            return key1.getKey().compareTo(key2.getKey());
        }
    }

    public static class CompositeKeyComparator extends WritableComparator implements Configurable {

        private ArrayList<int[]> orderMode = new ArrayList<int[]>();
        private Configuration conf;

        protected CompositeKeyComparator() {
            super(CompositeKey.class, true);
        }

        @Override
        public void setConf(Configuration conf) {
            this.conf = conf;
            String[] modeArr = conf.get("order.mode", "").split(",");
            for (String mode : modeArr) {
                String[] split = mode.split(":");
                if (split.length == 3) {
                    int[] record = {Integer.parseInt(split[0]), Integer.parseInt(split[1]), Integer.parseInt(split[2])};
                    orderMode.add(record);
                }
            }
        }

        @Override
        public Configuration getConf() {
            return conf;
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {

            CompositeKey compositekey1 = (CompositeKey) w1;
            CompositeKey compositekey2 = (CompositeKey) w2;
            String key1 = (compositekey1).getKey();
            String key2 = (compositekey2).getKey();

            int cmp = key1.compareTo(key2);
            if (cmp != 0) {
                return cmp;
            }

            String value1 = (compositekey1).getValue();
            String value2 = (compositekey2).getValue();

            String[] fields1 = value1.split("\\|", -1);
            String[] fields2 = value2.split("\\|", -1);

            for (int[] mode : orderMode) {
                if (mode[2] == 0) {
                    double orderByField1 = Double.parseDouble(fields1[mode[0]]);
                    double orderByField2 = Double.parseDouble(fields2[mode[0]]);
                    if (mode[1] == 0) {
                        if (orderByField1 > orderByField2) {
                            return 1;
                        } else if (orderByField1 < orderByField2) {
                            return -1;
                        }
                    } else {
                        if (orderByField2 > orderByField1) {
                            return 1;
                        } else if (orderByField2 < orderByField1) {
                            return -1;
                        }
                    }
                } else {
                    String orderByField1 = fields1[mode[0]];
                    String orderByField2 = fields2[mode[0]];

                    if (mode[1] == 0) {
                        if (orderByField1.compareTo(orderByField2) > 0) {
                            return 1;
                        } else if (orderByField1.compareTo(orderByField2) < 0) {
                            return -1;
                        }
                    } else {
                        if (orderByField2.compareTo(orderByField1) > 0) {
                            return 1;
                        } else if (orderByField2.compareTo(orderByField1) < 0) {
                            return -1;
                        }
                    }
                }
            }
            return value1.compareTo(value2);
        }
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
