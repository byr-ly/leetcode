package com.eb.bi.rs.andedu.theEntropyMethod;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

/**calculate
 * Created by linwanying on 2016/12/2.
 */
public class ScoreMapper extends Mapper<Object, Text, Text, Text> {
    private int action_num;
    private double[] eArr;
    private double size;
    /**
     * 输入格式：用户|产品|指标1|指标2|指标3（标准化处理后）
     */
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        String[] fields = value.toString().split("\\|");
        String userID = fields[0];
        String productID = fields[1];
        double score = 0, esum = 0, lnsize = Math.log(size);
        for (double ei : eArr) {
            esum += ei;
        }
        for (int i = 0; i < action_num; ++i) {
            score += (1 - eArr[i]/lnsize)/esum * Double.valueOf(fields[i+2]);
        }
        context.write(new Text(userID), new Text(productID + "," + score));
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException{
        Configuration conf = context.getConfiguration();
        action_num = Integer.valueOf(conf.get("action_num"));
        eArr = new double[action_num];
        URI[]  localFiles = context.getCacheFiles();
        if(localFiles == null){
            System.out.println("用户行为权重读取失败。");
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
                    size = Double.valueOf(values[action_num]);
                    for (int i = 0; i < action_num; ++i) {
                        eArr[i] = Double.valueOf(values[i]);
                    }
                }
            } finally {
                if (in != null) {
                    in.close();
                }
            }
        }
    }
}