package com.eb.bi.rs.frame2.algorithm.dataPreprocessing.theEntropyMethod;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**calculate sum of indexes
 * Created by linwanying on 2016/12/2.
 */
public class SumMapper extends Mapper<Object, Text, Text, Text> {
    /**
     * 输入格式：用户|目标|指标1|指标2|指标3|...
     */
    private Text keyOut = new Text("1");
    private Text valueOut = new Text();
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        String[] fields = value.toString().split("\\|");
        valueOut.set(value.toString().substring(fields[0].length() + fields[1].length() + 2));
        context.write(keyOut, valueOut);
    }
}
