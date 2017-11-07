package com.eb.bi.rs.andedu.theEntropyMethod;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

/**calculate the entropy of indexes
 * Created by linwanying on 2016/12/2.
 */
public class EntropyReducer extends Reducer<Text, Text, Text, Text> {
    //    Logger log = Logger.getLogger("EntropyReducer");
    private int action_num;
    private double[] arrSum;

    @Override
    public void setup(Context context) throws IOException, InterruptedException{
        Configuration conf = context.getConfiguration();
        action_num = Integer.valueOf(conf.get("action_num"));
        arrSum = new double[action_num];
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
                    String fields[] = line.split("\\s+");
                    if (fields.length != 2) continue;
                    String values[] = fields[1].split("\\|");
                    if (values.length != action_num) continue;
                    for (int i = 0; i < action_num; ++i) {
                        arrSum[i] = Double.valueOf(values[i]);
                    }
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
        double[] eArr = new double[action_num + 1];
//        double lnsize = Math.log(size);
        String keyOut = "";
        for (Text value : values) {
            String[] fields = value.toString().split("\\|");
            for (int i = 0; i < action_num; ++i) {
                double fi = 0;
                if (Double.parseDouble(fields[i]) != 0) {
                    fi = Double.parseDouble(fields[i]) / arrSum[i];
                    eArr[i] += -fi * Math.log(fi);
                }
                ++eArr[action_num];
            }
        }
        for (double ei : eArr) {
            keyOut += String.valueOf(ei) + "|";
        }
//        double esum = 0;
//        for (double ei : eArr) {
//            esum += 1 - ei/lnsize;
//        }
//        for (double ei : eArr) {
//            ei = (1 - ei / lnsize) / esum;
//            keyOut += String.valueOf(ei) + "|";
//        }
        context.write(new Text(key), new Text(keyOut));
    }
}