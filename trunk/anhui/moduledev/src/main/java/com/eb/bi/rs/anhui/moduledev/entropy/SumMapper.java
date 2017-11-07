package com.eb.bi.rs.anhui.moduledev.entropy;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**对用户行为进行求和
 * Created by linwanying on 2016/11/16.
 */
public class SumMapper extends Mapper<Object, Text, Text, Text> {
    /**
     * 输入格式：用户|品牌|是否连续更换改品牌|品牌忠诚度|品牌流行度（标准化处理后）
     */
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        String[] fields = value.toString().split("\\|");
        String is_change = fields[2];
        String loyalty = fields[3];
        String pop = fields[4];
        context.write(new Text("1"), new Text(is_change + "|" + loyalty + "|" + pop));
    }
}
