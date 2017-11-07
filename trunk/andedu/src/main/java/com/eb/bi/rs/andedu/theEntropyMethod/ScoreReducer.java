package com.eb.bi.rs.andedu.theEntropyMethod;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**score reducer
 * Created by linwanying on 2016/12/2.
 */
public class ScoreReducer extends Reducer<Text, Text, Text, NullWritable> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String info = key.toString();
        for (Text value : values) {
        	String[] result =value.toString().split(",");
        	if (result.length != 2) {
				return;
			}
        	context.write(new Text(info + "|" + result[0] + "|" + result[1]), NullWritable.get());
        }
    }
}
