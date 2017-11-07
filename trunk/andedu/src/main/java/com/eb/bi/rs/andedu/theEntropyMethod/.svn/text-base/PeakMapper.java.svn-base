package com.eb.bi.rs.andedu.theEntropyMethod;

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
        if (fields.length != action_num + 2) {
            return;
        }
        String keyOut = value.toString().substring(fields[0].length() + fields[1].length() + 2);
        context.write(new Text("1"), new Text(keyOut + "|" + keyOut));
    }
}
