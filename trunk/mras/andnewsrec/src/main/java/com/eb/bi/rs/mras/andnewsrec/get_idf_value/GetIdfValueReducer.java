package com.eb.bi.rs.mras.andnewsrec.get_idf_value;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
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
        for(IntWritable val:values){
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
        Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);

        if (localFiles == null) {
            System.out.println("没有找到缓存信息File ");
            return;
        }
        for (int i = 0; i < localFiles.length; i++) {
            System.out.println("localFile: " + localFiles[i]);
            String line;
            BufferedReader br = new BufferedReader(new FileReader(localFiles[i].toString()));

            String fileName = localFiles[i].toString();
            if (fileName.contains("part")) {
                //newsNum|number
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|", -1);
                    if (fields.length < 2) {
                        continue;
                    }
                    newsNum  = Integer.parseInt(fields[1]);
                }
                br.close();
                System.out.println("新闻数量加载成功 " + newsNum);
            }
        }
    }
}
