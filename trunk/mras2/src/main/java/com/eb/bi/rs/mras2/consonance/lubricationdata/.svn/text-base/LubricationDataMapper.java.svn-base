package com.eb.bi.rs.mras2.consonance.lubricationdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by houmaozheng on 2017/6/23.
 */
public class LubricationDataMapper extends Mapper<Object, Text, Text, Text> {
    private String fieldDelimiter;
    private int action_num;
    private int record_day;

    @Override
    protected void map(Object key, Text value,Context context) throws IOException,InterruptedException {
        String[] fields = value.toString().split("\\|");//用户ID | 物品ID | vst_pv | read_pv | dnld_cnt | book_read_ratio | 第一次听书时间first_day | 最后一次听书时间last_day

        if (fields.length >= 8 && checkRecordDay(fields[7])) {
            String userId = fields[0];
            String itemId = fields[1];
            String action = "";
            for ( int i = 2; i < 2+action_num; i++){
                action += fields[i] + "|";
            }

            context.write(new Text(userId + "|" + itemId), new Text(action));
        }
    }

    @Override
    protected void setup(Context context) throws IOException ,InterruptedException {
        Configuration conf = context.getConfiguration();
        fieldDelimiter = conf.get("field.delimiter", "\\|");
        action_num = Integer.parseInt(conf.get("action.num", "0"));
        record_day = Integer.parseInt(conf.get("record.day", "0"));
   }

   private boolean checkRecordDay(String last_day){

       SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
       try
       {
           Date today_date = new Date();
           Date last_date = format.parse(last_day);

           int days = (int) ((today_date.getTime() - last_date.getTime()) / (1000*3600*24));
           if (days < record_day) {
               return true;
           }
       } catch (ParseException e) {
           e.printStackTrace();
       }
       return false;
   }
}
