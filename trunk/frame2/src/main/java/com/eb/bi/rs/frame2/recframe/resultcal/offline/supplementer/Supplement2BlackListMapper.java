package com.eb.bi.rs.frame2.recframe.resultcal.offline.supplementer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Supplement2BlackListMapper extends Mapper<Text, Text, Text, Text> {
    private String dataformatType;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        dataformatType = context.getConfiguration().get("Appconf.data.input.format.type2");
    }

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        context.write(key, new Text("1|" + value.toString()));

        if (dataformatType.equals("s")) {
            context.write(key, new Text("1|" + value.toString()));
        } else {
            String[] fields = value.toString().split("\\|");

            for (int i = 0; i != fields.length; i++) {
                context.write(key, new Text("1|" + fields[i]));
            }
        }
    }
}
