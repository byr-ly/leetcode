package com.eb.bi.rs.mras.authorrec.itemcf.author_average_score;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by liyang on 2016/3/9.
 */
public class AuthorAveScoreReducer extends Reducer<Text,DoubleWritable,Text,NullWritable>{
    @Override
    public void reduce(Text key,Iterable<DoubleWritable> values,Context context)
        throws IOException,InterruptedException{
        double sumScore = 0.0;
        int number = 0;
        for(DoubleWritable val:values){
            number++;
            sumScore += val.get();
        }
        double averageScore = 0.0;
        try {
            averageScore = sumScore / number;
            context.write(new Text(key + "|" + averageScore), NullWritable.get());
        }catch (ArithmeticException e){
            System.out.printf("该作家没有平均分！"+key);
        }
    }
}
