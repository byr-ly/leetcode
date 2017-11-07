package com.eb.bi.rs.anhui.moduledev.entropy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

/**
 * 求熵值
 * Created by linwanying on 2016/11/16.
 */
public class EntropyReducer extends Reducer<Text, Text, Text, NullWritable> {
//    Logger log = Logger.getLogger("EntropyReducer");
    private long size = 0;
    private double changeSum = 0;
    private double loySum = 0;
    private double popSum = 0;

    @Override
    public void setup(Context context) throws IOException, InterruptedException{
        Configuration conf = context.getConfiguration();
        URI[]  localFiles = context.getCacheFiles();
        if(localFiles == null){
            System.out.println("用户各行为总和读取失败。");
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
                    changeSum = Long.parseLong(fields[0]);
                    loySum = Double.parseDouble(fields[1]);
                    popSum = Double.parseDouble(fields[2]);
                    size = Long.parseLong(fields[3]);
                    System.out.println("set up: " + "changeSum-"
                            + changeSum + ", loySum-" + loySum + ", popSum-" + popSum + ", size-" + size);
                }
            } finally {
                if (in != null) {
                    in.close();
                }
            }
        }
    }
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double ei = 0, el = 0, ep = 0, lnsize = Math.log(size);
        for (Text value : values) {
            double fi = 0, fl = 0, fp = 0;
            String[] fields = value.toString().split("\\|");
            if (Double.parseDouble(fields[0]) != 0) {
                fi = Double.parseDouble(fields[0]) / changeSum;
                ei += -fi * Math.log(fi);
            }
            if (Double.parseDouble(fields[1]) != 0) {
                fl = Double.parseDouble(fields[1]) / loySum;
                el += -fl * Math.log(fl);
            }
            if (Double.parseDouble(fields[2]) != 0) {
                fp = Double.parseDouble(fields[2]) / popSum;
                ep += -fp * Math.log(fp);
            }
        }
        //计算指标熵值
        ei /= lnsize;
        el /= lnsize;
        ep /= lnsize;

        //求出指标权重
        double esum = 3 - ei - el - ep;
        double wi = (1 - ei) / esum;
        double wl = (1 - el) / esum;
        double wp = (1 - ep) / esum;
        context.write(new Text(wi+ "|" +wl + "|" + wp), NullWritable.get());
    }
}
