package com.eb.bi.rs.anhui.moduledev.entropy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

/**数据标准化
 * Created by linwanying on 2016/11/16.
 */
public class StandardizeMapper extends Mapper<Object, Text, Text, NullWritable>{
//    Logger log = Logger.getLogger("StandardizeMapper");
    private double loyaltymax = 0;
    private double loyaltymin = 1;
    private double popularitymax = 0;
    private double popularitymin = 1;
    private double loyalsub = 1;
    private double popsub = 1;

    @Override
    public void setup(Context context) throws IOException, InterruptedException{
        Configuration conf = context.getConfiguration();
        URI[]  localFiles = context.getCacheFiles();
        if(localFiles == null){
            System.out.println("忠诚度和流行度最大值最小值读取失败。");
            return;
        }
        for (URI path : localFiles) {
            String line;
            BufferedReader in = null;
            try {
                FileSystem fs = FileSystem.get(path, conf);
                in = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
                while ((line = in.readLine()) != null) {
                    String fields[] = line.split("\\|");
                    if (fields.length != 4) continue;
                    loyaltymax = Double.parseDouble(fields[0]);
                    loyaltymin = Double.parseDouble(fields[1]);
                    popularitymax = Double.parseDouble(fields[2]);
                    popularitymin = Double.parseDouble(fields[3]);
                }
            } finally {
                if (in != null) {
                    in.close();
                }
            }
        }
        loyalsub = loyaltymax - loyaltymin;
        popsub = popularitymax - popularitymin;
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        String[] fields = value.toString().split("\\|");
        String userID = fields[0];
        String brandID = fields[1];
        String is_change = fields[2];
        double loyalty = Double.parseDouble(fields[3]);
        double pop = Double.parseDouble(fields[4]);
        loyalty = (loyalty - loyaltymin) / loyalsub;
        pop = (pop - popularitymin) / popsub;
        context.write(new Text(userID + "|" + brandID + "|" + is_change + "|" + loyalty + "|" + pop), NullWritable.get());
    }
}
