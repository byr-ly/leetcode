package com.eb.bi.rs.mras2.consonance.lubricationdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;


/**
 * Created by houmaozheng on 2017/6/23.
 */
public class LubricationDataReducer extends Reducer<Text, Text, NullWritable, Text> {

    private String fieldDelimiter;
    private int action_num;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        fieldDelimiter = conf.get("field.delimiter", "\\|");
        action_num = Integer.parseInt(conf.get("action.num", "0"));
    }

    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        DecimalFormat dcmFmt = new DecimalFormat("0.00000");

        for (Text pair : values) {
            String[] fields = pair.toString().split(fieldDelimiter);
            if (fields.length == action_num) {
                String output = "";
                for ( int i = 0; i < fields.length; i++ ) {
                    double temp = Math.log(Double.parseDouble(fields[i]));
                    if ( Double.isInfinite(temp) || Double.isNaN(temp)){
                        temp = 0;
                    }

                    output += dcmFmt.format(temp) + "|";
                }

                output = key.toString() + "|" + output.substring(0,output.length()-1);
                context.write(NullWritable.get(), new Text(output));
            }
        }
    }
}
