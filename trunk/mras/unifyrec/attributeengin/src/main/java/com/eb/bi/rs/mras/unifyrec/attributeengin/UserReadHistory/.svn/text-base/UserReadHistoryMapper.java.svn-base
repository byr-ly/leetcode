package com.eb.bi.rs.mras.unifyrec.attributeengin.UserReadHistory;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by LiuJie on 2016/04/10.
 */
public class UserReadHistoryMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\\|", -1);
        if (fields.length < 2) {
            return ;
        }
        String msisdn = fields[0].trim();
        String bookid = fields[1].trim();
        if (bookid.trim().isEmpty()) {
        	return;
        }
        context.write(new Text(msisdn), new Text(bookid));
    }
}
