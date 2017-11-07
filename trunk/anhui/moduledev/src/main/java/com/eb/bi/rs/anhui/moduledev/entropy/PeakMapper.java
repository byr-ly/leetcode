package com.eb.bi.rs.anhui.moduledev.entropy;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**求忠诚度和流行度的最大最小值
 * Created by linwanying on 2016/11/15.
 */
public class PeakMapper extends Mapper<Object, Text, Text, Text> {
    /**
     * 输入格式： 用户/品牌/是否连续更换改品牌/品牌忠诚度/品牌流行度
     */
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\\|");
        if (fields.length != 5) {
            return;
        }

//        String userID = fields[0];
//        String brandID = fields[1];
//        String is_change = fields[2];
        String loyalty = fields[3];
        String popularity = fields[4];
        context.write(new Text("1"), new Text(loyalty + "|" + popularity));
    }
}

