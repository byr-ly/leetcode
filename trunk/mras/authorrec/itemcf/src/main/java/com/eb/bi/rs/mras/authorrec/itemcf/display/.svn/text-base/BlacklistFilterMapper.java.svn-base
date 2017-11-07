package com.eb.bi.rs.mras.authorrec.itemcf.display;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class BlacklistFilterMapper
        extends Mapper<Object, Text, Text, Text> {
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
    private String currDateStr = null;
    private Date currDate = null;

    /**
     * @param value: 展示黑名单
     *               格式：msisdn|bookid|recorday
     *               只保留近5天的黑名单数据
     *               map out:
     *               格式：key:msisdn|bookid ; value:recorday
     */
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] strs = value.toString().split("\\|");
        String msisdn = strs[0];
        String bookid = strs[1];
        String recordDayStr = strs[2];
        try {
            Date recordDate = dateFormat.parse(recordDayStr);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(currDate);
            calendar.add(Calendar.DAY_OF_YEAR, -5);
            Date before5Date = calendar.getTime();
            if (!recordDate.after(before5Date)) {
                return;
            }
            String keyOut = String.format("%s|%s", msisdn, bookid);
            context.write(new Text(keyOut), new Text(recordDayStr));
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        currDateStr = context.getConfiguration().get("DATESTR");
        if (currDateStr != null && currDateStr.length() > 0) {
            try {
                currDate = dateFormat.parse(currDateStr);
            } catch (ParseException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                currDate = null;
            }
        }
        if (currDate == null) {
            currDate = new Date();
            String str = dateFormat.format(currDate);
            currDateStr = str;
        }
    }
}
