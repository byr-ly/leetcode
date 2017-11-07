package com.eb.bi.rs.anhui.moduledev.entropy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

/**求用户品牌综合得分
 * Created by linwanying on 2016/11/16.
 */
public class ScoreMapper extends Mapper<Object, Text, Text, Text> {
    private double wi = 0;
    private double wl = 0;
    private double wp = 0;
    /**
     * 输入格式：用户|品牌|是否连续更换改品牌|品牌忠诚度|品牌流行度（标准化处理后）
     */
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        String[] fields = value.toString().split("\\|");
        String userID = fields[0];
        String brandID = fields[1];
        int is_change = Integer.parseInt(fields[2]);
        double loyalty = Double.parseDouble(fields[3]);
        double pop = Double.parseDouble(fields[4]);
        String score = String.valueOf(wi * is_change + wl * loyalty + wp * pop);
        context.write(new Text(userID), new Text(brandID + "," + score));
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException{
        Configuration conf = context.getConfiguration();
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
                    String fields[] = line.split("\\|");
                    if (fields.length != 3) continue;
                    wi = Double.parseDouble(fields[0]);
                    wl = Double.parseDouble(fields[1]);
                    wp = Double.parseDouble(fields[2]);
                }
            } finally {
                if (in != null) {
                    in.close();
                }
            }
        }
    }
}
