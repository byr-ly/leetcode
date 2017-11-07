package com.eb.bi.rs.mras2.unifyrec.UserReadClasses;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by LiMingji on 2015/11/10.
 */
public class UserReadClassesMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\\|", -1);
        if (fields.length < 2) {
            return ;
        }
        String msisdn = fields[0].trim();
        String classes = fields[1].trim();
        if (classes.trim().isEmpty()) {
        	return;
        }
        context.write(new Text(msisdn), new Text(classes));
    }
}
