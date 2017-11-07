package com.eb.bi.rs.frame2.algorithm.dataPreprocessing.theEntropyMethod;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**score reducer
 * Created by linwanying on 2016/12/2.
 */
public class ScoreReducer extends Reducer<Text, Text, Text, NullWritable> {
    private Text keyOut = new Text();
    private NullWritable valueOut = NullWritable.get();
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            keyOut.set(key.toString() + "|" + value.toString());
            context.write(keyOut, valueOut);
        }
    }
}
