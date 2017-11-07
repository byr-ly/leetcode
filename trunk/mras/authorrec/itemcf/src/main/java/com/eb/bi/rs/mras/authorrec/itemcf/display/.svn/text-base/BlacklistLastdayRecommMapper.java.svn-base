package com.eb.bi.rs.mras.authorrec.itemcf.display;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class BlacklistLastdayRecommMapper
        extends Mapper<Object, Text, Text, Text> {
    private String currDateStr = null;

    /**
     * @param value: 前一天推荐结果
     *               格式：msisdn|authorid|bookid|type
     *               map out:
     *               格式：key: msisdn|bookid|recorday ; value:
     */
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] strs = value.toString().split("\\|");
        String msisdn = strs[0];
        String bookid = strs[2];
        String keyOut = String.format("%s|%s", msisdn, bookid);
        context.write(new Text(keyOut), new Text(currDateStr));
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        currDateStr = context.getConfiguration().get("DATESTR");
        if (currDateStr == null || currDateStr.length() == 0) {
            SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
            String str = format.format(new Date());
            currDateStr = str;
        }
    }
}
