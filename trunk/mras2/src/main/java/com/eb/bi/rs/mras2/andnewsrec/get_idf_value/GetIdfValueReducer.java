package com.eb.bi.rs.mras2.andnewsrec.get_idf_value;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.DecimalFormat;

/**
 * Created by liyang on 2016/5/31.
 */
public class GetIdfValueReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

    private int newsNum;

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        double idf = Math.log(newsNum) - Math.log(sum + 1);
        DecimalFormat df = new DecimalFormat("0.0000");
        double idfValue = Double.parseDouble(df.format(idf));
        context.write(new Text(key.toString() + "|" + String.valueOf(idfValue)), NullWritable.get());
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        System.out.printf("reduce setup");
        Configuration conf = context.getConfiguration();

        URI[] localFiles = context.getCacheFiles();
        for (int i = 0; i < localFiles.length; ++i) {
            String line;
            BufferedReader in = null;
            try {
                /*DistributedCache修改点*/
                FileSystem fs = FileSystem.get(localFiles[i], conf);
                in = new BufferedReader(new InputStreamReader(fs.open(new Path(localFiles[i]))));

                if (localFiles[i].toString().contains("part")) {
                    while ((line = in.readLine()) != null) {//newsNum|number
                        String[] fields = line.split("\\|", -1);
                        if (fields.length < 2) {
                            continue;
                        }
                        newsNum = Integer.parseInt(fields[1]);
                    }
                    System.out.println("新闻数量加载成功 " + newsNum);
                }
            } finally {
                if (in != null) {
                    in.close();
                }
            }
        }
    }
}
