package com.eb.bi.rs.frame2.algorithm.dataPreprocessing.theEntropyMethod;

/**Extract user behavior
 * Created by linwanying on 2016/12/1.
 */

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**求忠诚度和流行度的最大最小值
 * Created by linwanying on 2016/11/15.
 */
public class PeakMapper extends Mapper<Object, Text, Text, Text> {
    /**
     * 输入格式： 用户/品牌/行为1|行为2|...
     */
    private Logger log = Logger.getLogger("PeakMapper");
    private Text keyOut = new Text("1");
    private Text valueOut = new Text();
    private int action_num;
    private String separator;
    @Override
    protected void setup(Context context) throws IOException,InterruptedException {
        action_num = Integer.valueOf(context.getConfiguration().get("action_num"));
        separator = context.getConfiguration().get("separator");
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields;
        if (separator.equals("|")) {
            fields = value.toString().split("\\|");
        } else {
            fields = value.toString().split(separator);
        }
        if (fields.length < action_num + 2) {
            return;
        }
        StringBuilder keyout = new StringBuilder();
        for (int i = 0; i < action_num; ++i) {
            keyout.append(fields[i+2]).append("|");
        }
        valueOut.set(keyout.append(keyout).toString());
        log.info(valueOut);
        context.write(keyOut, valueOut);
    }
}
